#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


import sys

from airbyte_cdk.entrypoint import launch
from source_planhat import SourcePlanhat

if __name__ == "__main__":
    source = SourcePlanhat()
    launch(source, sys.argv[1:])