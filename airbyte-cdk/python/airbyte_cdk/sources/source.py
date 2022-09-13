#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#


import json
import logging
from abc import ABC, abstractmethod
from collections import defaultdict
from typing import Any, Generic, Iterable, List, Mapping, MutableMapping, TypeVar, Union

from airbyte_cdk.connector import BaseConnector, DefaultConnectorMixin, TConfig
from airbyte_cdk.models import AirbyteCatalog, AirbyteMessage, AirbyteStateMessage, AirbyteStateType, ConfiguredAirbyteCatalog

TState = TypeVar("TState")
TCatalog = TypeVar("TCatalog")


class BaseSource(BaseConnector[TConfig], ABC, Generic[TConfig, TState, TCatalog]):
    @abstractmethod
    def read_state(self, state_path: str) -> TState:
        ...

    @abstractmethod
    def read_catalog(self, catalog_path: str) -> TCatalog:
        ...

    @abstractmethod
    def read(self, logger: logging.Logger, config: TConfig, catalog: TCatalog, state: TState = None) -> Iterable[AirbyteMessage]:
        """
        Returns a generator of the AirbyteMessages generated by reading the source with the given configuration, catalog, and state.
        """

    @abstractmethod
    def discover(self, logger: logging.Logger, config: TConfig) -> AirbyteCatalog:
        """
        Returns an AirbyteCatalog representing the available streams and fields in this integration. For example, given valid credentials to a
        Postgres database, returns an Airbyte catalog where each postgres table is a stream, and each table column is a field.
        """


class Source(
    DefaultConnectorMixin,
    BaseSource[Mapping[str, Any], Union[List[AirbyteStateMessage], MutableMapping[str, Any]], ConfiguredAirbyteCatalog],
    ABC,
):
    # can be overridden to change an input state
    def read_state(self, state_path: str) -> Union[List[AirbyteStateMessage], MutableMapping[str, Any]]:
        """
        Retrieves the input state of a sync by reading from the specified JSON file. Incoming state can be deserialized into either
        a JSON object for legacy state input or as a list of AirbyteStateMessages for the per-stream state format. Regardless of the
        incoming input type, it will always be transformed and output as a list of AirbyteStateMessage(s).
        :param state_path: The filepath to where the stream states are located
        :return: The complete stream state based on the connector's previous sync
        """
        if state_path:
            state_obj = json.loads(open(state_path, "r").read())
            if not state_obj:
                return self._emit_legacy_state_format({})
            is_per_stream_state = isinstance(state_obj, List)
            if is_per_stream_state:
                parsed_state_messages = []
                for state in state_obj:
                    parsed_message = AirbyteStateMessage.parse_obj(state)
                    if not parsed_message.stream and not parsed_message.data and not parsed_message.global_:
                        raise ValueError("AirbyteStateMessage should contain either a stream, global, or state field")
                    parsed_state_messages.append(parsed_message)
                return parsed_state_messages
            else:
                return self._emit_legacy_state_format(state_obj)
        return self._emit_legacy_state_format({})

    def _emit_legacy_state_format(self, state_obj) -> Union[List[AirbyteStateMessage], MutableMapping[str, Any]]:
        """
        Existing connectors that override read() might not be able to interpret the new state format. We temporarily
        send state in the old format for these connectors, but once all have been upgraded, this method can be removed,
        and we can then emit state in the list format.
        """
        # vars(self.__class__) checks if the current class directly overrides the read() function
        if "read" in vars(self.__class__):
            return defaultdict(dict, state_obj)
        else:
            if state_obj:
                return [AirbyteStateMessage(type=AirbyteStateType.LEGACY, data=state_obj)]
            else:
                return []

    # can be overridden to change an input catalog
    def read_catalog(self, catalog_path: str) -> ConfiguredAirbyteCatalog:
        return ConfiguredAirbyteCatalog.parse_obj(self.read_config(catalog_path))
