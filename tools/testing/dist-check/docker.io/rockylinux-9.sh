#!/bin/bash -e

#
# Copyright (C) 2020-present ScyllaDB
#

#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

source "$(dirname $0)/util.sh"

echo "Installing Scylla ($MODE) packages on $PRETTY_NAME..."

dnf update -y -q
dnf install -y "${SCYLLA_RPMS[@]}"
