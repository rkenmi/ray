import copy
import json
import logging
import time
from typing import Dict, Any, List

from botocore.exceptions import ClientError

from ray.autoscaler._private.aws.sns.sns_helper import SnsHelper
from ray.autoscaler._private.cli_logger import cli_logger
from ray.autoscaler._private.event_system import RayEvent
from ray.autoscaler._private.updater import NodeContext

from ray.autoscaler._private.event_system import EventPublisher, EventCallbackHandler

logger = logging.getLogger(__name__)


class AwsEventPublisher(EventPublisher):
    def __init__(self, events_config: Dict[str, Any]):
        super().__init__(events_config)
        self.uri = events_config["notification_uri"]
        self.parameters = events_config.get("parameters", {})

    @property
    def config(self) -> Dict[str, Any]:
        return self.events_config

    @property
    def params(self) -> Dict[str, Any]:
        return self.parameters

    def validate_config(self, events_config: Dict[str, Any]):
        notification_uri = events_config["notification_uri"]
        assert notification_uri is not None, f"`notification_uri` is a required field in `events`"
        assert notification_uri.startswith("arn:aws"), f"Invalid ARN specified: {notification_uri}"

    def validate_params(self, params_config: Dict[str, Any]):
        assert params_config is not None

    def get_callback_handlers(self) -> List[EventCallbackHandler]:
        """Get callback handlers based on the provided AWS ARN.

        :return: list of callback handlers with their corresponding Callable arguments and keyword arguments
        """
        # TODO: Add support for multiple URI
        handlers = []
        if self.uri.startswith("arn:aws:sns"):
            handlers.append(EventCallbackHandler(self._sns_callback, SnsHelper(self._get_region()), **self.parameters))
        elif self.uri.startswith("arn:aws:lambda"):
            handlers.append(EventCallbackHandler(self._lambda_callback, None, **self.parameters))
        elif self.uri.startswith("arn:aws:logs"):
            handlers.append(EventCallbackHandler(self._cloudwatch_callback, None, **self.parameters))
        elif self.uri.startswith("arn:aws:apigateway"):
            handlers.append(EventCallbackHandler(self._api_gateway_callback, None, **self.parameters))

        return handlers

    def _construct_sns_message(self, event_data: Dict[str, Any], **kwargs) -> Dict[str, Any]:
        # create a copy of the event data to modify
        event_dict = copy.deepcopy(event_data)
        event: RayEvent = event_dict.pop("event")
        node_context: NodeContext = event_dict.get("node_context", {})
        sns_topic_arn, params = self.uri, kwargs
        custom_description = event_dict.get("customDescription")
        custom_event_name = event_dict.get("customEventName")
        ray_parent_session_id = event_dict.get("rayParentSessionId")
        ray_session_id = event_dict.get("raySessionId")
        message = {
            **params,
            "state": event.state,
            "stateSequence": event.value - 1,  # zero-index sequencing
            "stateDetailStatus": "SUCCESS",
            "rayParentSessionId": ray_parent_session_id,
            "raySessionId": ray_session_id,
            "timestamp": round(time.time() * 1000),
        }

        if custom_event_name:
            message["eventName"] = custom_event_name

        if custom_description:
            message["stateDetailDescription"] = custom_description

        if node_context:
            message["rayNodeId"] = node_context["node_id"]
            message["rayNodeType"] = "HEAD" if node_context["is_head_node"] else "WORKER"

        return message

    def _sns_callback(self, sns_client: SnsHelper, event_data: Dict[str, Any], **kwargs):
        """SNS callback for sending Ray cluster event data to an SNS topic.

        Args:
            sns_client: Amazon SNS client for publishing to an SNS topic
            event_data: Ray cluster setup event data. This contains the event name, enum ID, and
                may also contain additional metadata (i.e. the initialization or setup command used
                during this setup step)
            **kwargs: Keyword arguments injected into `_EventSystem.add_callback_handler` before initialization
        """
        event: RayEvent = event_data.get("event")
        sns_topic_arn, params = self.uri, kwargs
        message = self._construct_sns_message(event_data, **kwargs)
        try:
            sns_client.publish(sns_topic_arn, json.dumps(message))
            logger.info("Published SNS event {} to {}".format(event.name, sns_topic_arn))
        except ClientError as exc:
            cli_logger.abort(
                "{} Error caught when publishing {} create cluster events to SNS",
                exc.response["Error"], event.name)

    def _lambda_callback(self):
        raise NotImplementedError("AWS Lambda callback is not implemented")

    def _cloudwatch_callback(self):
        raise NotImplementedError("AWS Cloudwatch callback is not implemented")

    def _api_gateway_callback(self):
        raise NotImplementedError("AWS API Gateway callback is not implemented")

    def _get_region(self) -> str:
        return self.uri.split(":")[3]
