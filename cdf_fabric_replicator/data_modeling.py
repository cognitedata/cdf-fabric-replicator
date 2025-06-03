import json
import logging
import os
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import pyarrow as pa
from cognite.client.data_classes.data_modeling.ids import ViewId
from cognite.client.data_classes.data_modeling.query import (
    EdgeResultSetExpression,
    NodeResultSetExpression,
    Query,
    QueryResult,
    Select,
    SourceSelector,
)
from cognite.client.data_classes.filters import Equals, HasData
from cognite.client.exceptions import CogniteAPIError
from cognite.extractorutils.base import CancellationToken, Extractor
from deltalake import write_deltalake
from deltalake.exceptions import DeltaError

from cdf_fabric_replicator import __version__
from cdf_fabric_replicator.config import Config, DataModelingConfig
from cdf_fabric_replicator.metrics import Metrics


class DataModelingReplicator(Extractor):
    """Streams CDF Data-Modeling instances into S3-based Delta tables."""

    # ─────────────────────────── setup ───────────────────────────
    def __init__(
        self,
        metrics: Metrics,
        stop_event: CancellationToken,
        override_config_path: Optional[str] = None,
    ):
        super().__init__(
            name="cdf_fabric_replicator_data_modeling",
            description="CDF → Delta-Lake (S3)",
            config_class=Config,
            metrics=metrics,
            use_default_state_store=False,
            version=__version__,
            cancellation_token=stop_event,
            config_file_path=override_config_path,
        )
        self.stop_event = stop_event
        self.logger = logging.getLogger(self.name)

        self.s3_cfg = None
        self.base_dir: Path = Path.cwd() / "deltalake"
        self.base_dir.mkdir(parents=True, exist_ok=True)

    # ─────────────────────────── main loop ──────────────────────
    def run(self) -> None:
        self.s3_cfg = (
            self.config.destination.s3 if self.config.destination else None
        )
        if not self.s3_cfg:
            raise RuntimeError("destination.s3 must be configured")
        self.state_store.initialize()
        if not self.config.data_modeling:
            self.logger.info("No data-modeling spaces configured — exiting.")
            return

        while not self.stop_event.is_set():
            t0 = time.time()
            self.process_spaces()
            # wait until poll_time has elapsed
            delay = max(self.config.extractor.poll_time - (time.time() - t0), 0)
            if delay:
                self.stop_event.wait(delay)

    # ───────────────────────── processing ───────────────────────
    def process_spaces(self) -> None:
        for dm_cfg in self.config.data_modeling:
            try:
                all_views = self.cognite_client.data_modeling.views.list(
                    space=dm_cfg.space, limit=-1
                )
            except CogniteAPIError as err:
                self.logger.error("View-list failed for %s: %s", dm_cfg.space, err)
                continue

            for v in all_views.dump():
                self._process_instances(dm_cfg, f"{dm_cfg.space}_{v['externalId']}_{v['version']}", v)

            self._process_instances(dm_cfg, f"{dm_cfg.space}_edges")

    # .....................................................................
    def _process_instances(
        self,
        dm_cfg: DataModelingConfig,
        state_id: str,
        view: Dict[str, Any] | None = None,
    ) -> None:
        query = (
            self._q_for_view(view) if view else self._q_for_edge()
        )
        self._iterate_and_write(dm_cfg, state_id, query)

    # ───────── query helpers ────────────────────────────────────
    @staticmethod
    def _q_for_view(view: Dict[str, Any]) -> Query:
        props = list(view["properties"])
        vid = ViewId(view["space"], view["externalId"], view["version"])

        if view["usedFor"] != "edge":
            return Query(
                with_={"nodes": NodeResultSetExpression(filter=HasData([vid]))},
                select={"nodes": Select([SourceSelector(vid, props)])},
            )
        return Query(
            with_={
                "edges": EdgeResultSetExpression(
                    filter=Equals(["edge", "type"], {"space": vid.space, "externalId": vid.external_id})
                )
            },
            select={"edges": Select([SourceSelector(vid, props)])},
        )

    @staticmethod
    def _q_for_edge() -> Query:
        return Query(with_={"edges": EdgeResultSetExpression()}, select={"edges": Select()})

    # ───────── cursor loop / writer ─────────────────────────────
    def _iterate_and_write(self, dm_cfg: DataModelingConfig, state_id: str, query: Query) -> None:
        cursors = self.state_store.get_state(external_id=state_id)[1]
        if cursors:
            query.cursors = json.loads(str(cursors))

        while True:
            res = self.cognite_client.data_modeling.instances.sync(query=query)
            #self._send_to_s3(dm_cfg, res)
            self._send_to_local(dm_cfg, res)

            if not res.cursors:
                break
            query.cursors = res.cursors

        self.state_store.set_state(external_id=state_id, high=json.dumps(query.cursors))
        self.state_store.synchronize()

    '''
    def _send_to_s3(self, dm_cfg: DataModelingConfig, result: QueryResult) -> None:
        for tbl_name, rows in self._extract_instances(result).items():
            self._delta_append(tbl_name, rows, dm_cfg.space)
    '''

    def _send_to_local(self, dm_cfg: DataModelingConfig, result: QueryResult) -> None:
        for tbl_name, rows in self._extract_instances(result).items():
            self._delta_append(tbl_name, rows, dm_cfg.space)

    # ───────── extract rows ─────────────────────────────────────
    @staticmethod
    def _extract_instances(res: QueryResult) -> Dict[str, List[Dict[str, Union[str, int]]]]:
        instances: Dict[str, List[Dict[str, Union[str, int]]]] = {}

        for edges in res.data.get("edges", []):
            tbl = f"{edges.space}_edges"
            instances.setdefault(tbl, []).append(
                {
                    "space":          edges.space,
                    "instanceType":   "edge",
                    "externalId":     edges.external_id,
                    "version":        edges.version,
                    "startNode":      {"space": edges.start_node.space, "externalId": edges.start_node.external_id},
                    "endNode":        {"space": edges.end_node.space,   "externalId": edges.end_node.external_id},
                    "lastUpdatedTime": edges.last_updated_time,
                    "createdTime":     edges.created_time,
                    **{k: v for p in edges.properties.data.values() for k, v in p.items()},
                }
            )

        for nodes in res.data.get("nodes", []):
            for view_id, props in nodes.properties.data.items():
                tbl = f"{view_id.space}_{view_id.external_id}"
                instances.setdefault(tbl, []).append(
                    {
                        "space":           nodes.space,
                        "instanceType":    "node",
                        "externalId":      nodes.external_id,
                        "version":         nodes.version,
                        "lastUpdatedTime": nodes.last_updated_time,
                        "createdTime":     nodes.created_time,
                        **props,
                    }
                )

        return instances


    def _delta_append(self, table: str, rows: List[Dict[str, Any]], space: str) -> None:
        uri = (
            f"s3://{self.s3_cfg.bucket}/"
            f"{(self.s3_cfg.prefix or '')}{space}/tables/{table}"
        )
        self.logger.info("Δ-append %s rows → %s", len(rows), uri)

        try:
            '''
            write_deltalake(
                uri,
                pa.Table.from_pylist(rows),
                mode="append",
                engine="rust",
                schema_mode="merge",
                storage_options={
                    k: v
                    for k, v in {
                        "AWS_ACCESS_KEY_ID":     os.getenv("AWS_ACCESS_KEY_ID"),
                        "AWS_SECRET_ACCESS_KEY": os.getenv("AWS_SECRET_ACCESS_KEY"),
                        "AWS_REGION":            self.s3_cfg.region or os.getenv("AWS_REGION"),
                    }.items()
                    if v
                }
                or None,
            )

            write_deltalake(
                uri,
                pa.Table.from_pylist(rows),
                mode="append",
                engine="rust",
                schema_mode="merge",
                )
            '''
            uri = self.base_dir / space / "tables" / table
            uri.mkdir(parents=True, exist_ok=True)

            self.logger.info("Δ‑append %s rows → %s", len(rows), uri)
            write_deltalake(
                str(uri),
                pa.Table.from_pylist(rows),
                mode="append",
                engine="rust",
                schema_mode="merge",
            )
        except DeltaError as err:
            self.logger.error("Delta write failed: %s", err)
            raise
