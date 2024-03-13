#!/bin/bash
az login --identity --allow-no-subscriptions
exec cdf_fabric_replicator $*
