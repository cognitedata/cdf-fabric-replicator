# -*- mode: python ; coding: utf-8 -*-

import os
import sys

try:
    from cdf_fabric_replicator import __version__

    version = f"{__version__}-"
except ImportError:
    version = ""

if os.name == "nt":
    script_path = "cdf_fabric_replicator\\__main__.py"

else:
    script_path = "cdf_fabric_replicator/__main__.py"


hiddenimports = [
    'gssapi.raw._enum_extensions.ext_iov_mic',
    'gssapi.raw._enum_extensions.ext_dce',
    'gssapi.raw.cython_converters',
    'krb5',
    'gssapi.raw.inquire_sec_context_by_oid',
    'sspilib.raw._text',
    "boto3",
]

a = Analysis(
    [script_path],
    binaries=[],
    datas=[],
    hiddenimports=hiddenimports,
    hookspath=[],
    runtime_hooks=[],
    excludes=[],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=None,
    noarchive=False,
)
pyz = PYZ(a.pure, a.zipped_data, cipher=None)
exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.zipfiles,
    a.datas,
    [],
    name=f"fabric-connector-standalone-{version}{sys.platform}",
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=True,
    # icon="logo.ico",
)
