import os
import pprint
import sys
import time
import traceback
import urllib.parse
from datetime import datetime, timezone
from decimal import Decimal
from enum import IntEnum
from math import ceil, floor

datetime_format_1 = "%Y-%m-%dT%H:%M:%S.%fZ"
datetime_format_2 = "%Y-%m-%dT%H-%M-%S.%fZ"
datetime_format_3 = "%Y-%m-%dT%H:%M:%S"
datetime_format_4 = "%Y-%m-%dT%H-%M-%S"
datetime_format_5 = "%Y-%m-%dT%H:%M:%SZ"
datetime_format_6 = "%Y-%m-%dT%H-%M-%SZ"


class LogLevel(IntEnum):
    TRACE = 10
    DEBUG = 20
    FINE = 30
    DETAIL = 40
    INFO = 50
    WARNING = 60
    ERROR = 70
    CRITICAL = 80
    NONE = 90


class LoggerApi:

    def trace(self, *messages: str) -> None:
        raise NotImplementedError

    def debug(self, *messages: str) -> None:
        raise NotImplementedError

    def fine(self, *messages: str) -> None:
        raise NotImplementedError

    def detail(self, *messages: str) -> None:
        raise NotImplementedError

    def info(self, *messages: str) -> None:
        raise NotImplementedError

    def warning(self, *messages: str) -> None:
        raise NotImplementedError

    def error(self, exception: Exception) -> None:
        raise NotImplementedError

    def critical(self, exception: Exception) -> None:
        raise NotImplementedError


class Logger(LoggerApi):
    def __init__(self, *, level, name, datetime_format=datetime_format_1, sep="\n", end="\n\n", width=160, exit_on_error=False):
        self.level = level
        self.name = name
        self.message_format = "{} {} {{{}:{}:{}}} {}"
        self.datetime_format = datetime_format
        self.sep = sep
        self.end = end
        self.width = width
        self.whitespaces = 10 * " "
        self.exit_on_error = exit_on_error

    def trace(self, *messages: str) -> None:
        if self.level <= LogLevel.TRACE:
            current_datetime_str = datetime.now(timezone.utc).strftime(self.datetime_format)
            caller_frame = sys._getframe(1)
            self.write(
                current_datetime_str=current_datetime_str,
                message=self.message_format.format(
                    self.name,
                    datetime.now(timezone.utc).strftime(self.datetime_format),
                    os.path.basename(caller_frame.f_code.co_filename),
                    caller_frame.f_code.co_name,
                    caller_frame.f_lineno,
                    f"TRACE{self.whitespaces}{self.sep.join((self.serialize(object=x, width=self.width) for x in messages))}",
                ),
            )

    def debug(self, *messages: str) -> None:
        if self.level <= LogLevel.DEBUG:
            current_datetime_str = datetime.now(timezone.utc).strftime(self.datetime_format)
            caller_frame = sys._getframe(1)
            self.write(
                current_datetime_str=current_datetime_str,
                message=self.message_format.format(
                    self.name,
                    datetime.now(timezone.utc).strftime(self.datetime_format),
                    os.path.basename(caller_frame.f_code.co_filename),
                    caller_frame.f_code.co_name,
                    caller_frame.f_lineno,
                    f"DEBUG{self.whitespaces}{self.sep.join((self.serialize(object=x, width=self.width) for x in messages))}",
                ),
            )

    def fine(self, *messages: str) -> None:
        if self.level <= LogLevel.FINE:
            current_datetime_str = datetime.now(timezone.utc).strftime(self.datetime_format)
            caller_frame = sys._getframe(1)
            self.write(
                current_datetime_str=current_datetime_str,
                message=self.message_format.format(
                    self.name,
                    datetime.now(timezone.utc).strftime(self.datetime_format),
                    os.path.basename(caller_frame.f_code.co_filename),
                    caller_frame.f_code.co_name,
                    caller_frame.f_lineno,
                    f"FINE{self.whitespaces}{self.sep.join((self.serialize(object=x, width=self.width) for x in messages))}",
                ),
            )

    def detail(self, *messages: str) -> None:
        if self.level <= LogLevel.DETAIL:
            current_datetime_str = datetime.now(timezone.utc).strftime(self.datetime_format)
            caller_frame = sys._getframe(1)
            self.write(
                current_datetime_str=current_datetime_str,
                message=self.message_format.format(
                    self.name,
                    datetime.now(timezone.utc).strftime(self.datetime_format),
                    os.path.basename(caller_frame.f_code.co_filename),
                    caller_frame.f_code.co_name,
                    caller_frame.f_lineno,
                    f"DETAIL{self.whitespaces}{self.sep.join((self.serialize(object=x, width=self.width) for x in messages))}",
                ),
            )

    def info(self, *messages: str) -> None:
        if self.level <= LogLevel.INFO:
            current_datetime_str = datetime.now(timezone.utc).strftime(self.datetime_format)
            caller_frame = sys._getframe(1)
            self.write(
                current_datetime_str=current_datetime_str,
                message=self.message_format.format(
                    self.name,
                    datetime.now(timezone.utc).strftime(self.datetime_format),
                    os.path.basename(caller_frame.f_code.co_filename),
                    caller_frame.f_code.co_name,
                    caller_frame.f_lineno,
                    f"INFO{self.whitespaces}{self.sep.join((self.serialize(object=x, width=self.width) for x in messages))}",
                ),
            )

    def warning(self, *messages: str) -> None:
        if self.level <= LogLevel.WARNING:
            current_datetime_str = datetime.now(timezone.utc).strftime(self.datetime_format)
            caller_frame = sys._getframe(1)
            self.write(
                current_datetime_str=current_datetime_str,
                message=self.message_format.format(
                    self.name,
                    datetime.now(timezone.utc).strftime(self.datetime_format),
                    os.path.basename(caller_frame.f_code.co_filename),
                    caller_frame.f_code.co_name,
                    caller_frame.f_lineno,
                    f"WARNING{self.whitespaces}{self.sep.join((self.serialize(object=x, width=self.width) for x in messages))}",
                ),
            )

    def error(self, exception: Exception) -> None:
        if self.level <= LogLevel.ERROR:
            current_datetime_str = datetime.now(timezone.utc).strftime(self.datetime_format)
            caller_frame = sys._getframe(1)
            self.write(
                current_datetime_str=current_datetime_str,
                message=self.message_format.format(
                    self.name,
                    datetime.now(timezone.utc).strftime(self.datetime_format),
                    os.path.basename(caller_frame.f_code.co_filename),
                    caller_frame.f_code.co_name,
                    caller_frame.f_lineno,
                    "ERROR",
                ),
            )
            self.write(current_datetime_str=current_datetime_str, message=repr(exception))
            self.write(current_datetime_str=current_datetime_str, message=traceback.format_exc())
            if os.getenv("CRYPTO_TRADE_EXIT_ON_ERROR", "false").lower() == "true" or self.exit_on_error:
                sys.exit("exit")

    def critical(self, exception: Exception) -> None:
        if self.level <= LogLevel.CRITICAL:
            current_datetime_str = datetime.now(timezone.utc).strftime(self.datetime_format)
            caller_frame = sys._getframe(1)
            self.write(
                current_datetime_str=current_datetime_str,
                message=self.message_format.format(
                    self.name,
                    datetime.now(timezone.utc).strftime(self.datetime_format),
                    os.path.basename(caller_frame.f_code.co_filename),
                    caller_frame.f_code.co_name,
                    caller_frame.f_lineno,
                    "CRITICAL",
                ),
            )
            self.write(current_datetime_str=current_datetime_str, message=repr(exception))
            self.write(current_datetime_str=current_datetime_str, message=traceback.format_exc())
            sys.exit("exit")

    def serialize(self, *, object, width):
        if isinstance(object, (bool, str, int, float, type(None))):
            return str(object)
        elif isinstance(object, (RestRequest, RestResponse, WebsocketConnection, WebsocketMessage, WebsocketRequest)):
            return pprint.pformat(object.as_readable_dict(), width=width)
        else:
            return pprint.pformat(object, width=width)

    def write(self, *, current_datetime_str, message):
        sys.stdout.write(message)
        sys.stdout.write(self.end)


class LoggerWithWriter(Logger):
    def __init__(self, *, level, name, writer):
        super().__init__(level=level, name=name)
        self.writer = writer

    def write(self, *, current_datetime_str, message):
        self.writer.write(current_datetime_str=current_datetime_str, message=message)

    def close(self):
        self.writer.close()


class Writer:
    def __init__(
        self, *, end="\n\n", write_path=None, write_dir=None, write_current_datetime_str_key=None, write_extension=None, write_header=None, write_buffering=-1
    ):
        self.end = end
        self.write_file = None
        self.write_path = write_path
        self.write_dir = write_dir
        if write_current_datetime_str_key:
            self.write_current_datetime_str_key = write_current_datetime_str_key
        else:
            self.write_current_datetime_str_key = lambda current_datetime_str: current_datetime_str[:10]
        self.write_extension = write_extension or ".txt"
        self.write_header = write_header
        if self.write_path:
            os.makedirs(os.path.dirname(self.write_path), exist_ok=True)
        elif self.write_dir:
            os.makedirs(self.write_dir, exist_ok=True)
        self.write_buffering = write_buffering

    def write(self, *, current_datetime_str, message):
        if self.write_path:
            if not self.write_file:
                self.open(current_datetime_str_key="", write_path=self.write_path)
            self.write_file[1].write(message)
            self.write_file[1].write(self.end)
        elif self.write_dir:
            current_datetime_str_key = self.write_current_datetime_str_key(current_datetime_str)
            write_path = f"{self.write_dir}/{current_datetime_str_key}{self.write_extension}"
            if not self.write_file:
                self.open(current_datetime_str_key=current_datetime_str_key, write_path=write_path)
            else:
                if self.write_file[0] != current_datetime_str_key:
                    if not self.write_file[1].closed:
                        self.write_file[1].close()
                    self.open(current_datetime_str_key=current_datetime_str_key, write_path=write_path)
            self.write_file[1].write(message)
            self.write_file[1].write(self.end)
        else:
            sys.stdout.write(message)
            sys.stdout.write(self.end)

    def open(self, *, current_datetime_str_key, write_path):
        need_write_header = self.write_header and (not os.path.exists(write_path) or os.path.getsize(write_path) == 0)
        self.write_file = (current_datetime_str_key, open(write_path, "a", buffering=self.write_buffering))
        if need_write_header:
            self.write_file[1].write(self.write_header)
            self.write_file[1].write(self.end)

    def close(self):
        if self.write_file and self.write_file[1] and not self.write_file[1].closed:
            self.write_file[1].close()


class RestRequest:
    METHOD_GET = "GET"
    METHOD_HEAD = "HEAD"
    METHOD_POST = "POST"
    METHOD_PUT = "PUT"
    METHOD_DELETE = "DELETE"
    METHOD_CONNECT = "CONNECT"
    METHOD_OPTIONS = "OPTIONS"
    METHOD_TRACE = "TRACE"
    METHOD_PATCH = "PATCH"

    def __init__(
        self,
        *,
        id=None,
        base_url=None,
        method=None,
        path=None,
        query_params=None,
        query_string=None,
        payload=None,
        json_payload=None,
        json_serialize=None,
        headers=None,
        extra_data=None,
    ):
        self.id = id
        self.base_url = base_url
        self.method = method
        self.path = path
        self.query_params = query_params
        if query_params:
            self.query_string = "&".join([f"{k}={urllib.parse.quote_plus(str(v))}" for k, v in sorted(dict(query_params).items())])
        else:
            self.query_string = query_string
        self.headers = headers
        self.json_payload = json_payload
        if json_payload and json_serialize:
            self.payload = json_serialize(json_payload)
        else:
            self.payload = payload
        self.extra_data = extra_data

    def as_readable_dict(self):
        return self.__dict__

    @property
    def url(self):
        return create_url(base_url=self.base_url, path=self.path)

    @property
    def path_with_query_string(self):
        return create_path_with_query_string(path=self.path, query_string=self.query_string)


class RestResponse:
    def __init__(
        self,
        *,
        status_code=None,
        payload=None,
        headers=None,
        json_deserialize=None,
        rest_request=None,
        next_rest_request_function=None,
        next_rest_request_delay_seconds=0,
    ):
        self.status_code = status_code
        self.payload = payload
        self.headers = headers
        self.json_deserialized_payload = (
            json_deserialize(payload) if payload and headers["Content-Type"].startswith("application/json") and json_deserialize else None
        )
        self.rest_request = rest_request
        self.next_rest_request_function = next_rest_request_function
        self.next_rest_request_delay_seconds = next_rest_request_delay_seconds

    def as_readable_dict(self):
        return {
            "status_code": self.status_code,
            "payload": self.payload,
            "headers": dict(self.headers),
            "json_deserialized_payload": self.json_deserialized_payload,
            "rest_request": self.rest_request.as_readable_dict() if self.rest_request else None,
            "has_next_rest_request_function": self.next_rest_request_function is not None,
            "next_rest_request_delay_seconds": self.next_rest_request_delay_seconds,
        }


class WebsocketConnection:
    def __init__(self, *, base_url=None, path=None, query_params=None, connection=None):
        self.base_url = base_url
        self.path = path
        self.query_params = query_params
        self.connection = connection
        self.latest_receive_message_time_point = None

    def as_readable_dict(self):
        return {
            "base_url": self.base_url,
            "path": self.path,
            "query_params": self.query_params,
            "latest_receive_message_time_point": self.latest_receive_message_time_point,
        }

    @property
    def url_with_query_params(self):
        return create_url_with_query_params(base_url=self.base_url, path=self.path, query_params=self.query_params)


class WebsocketMessage:
    def __init__(
        self, *, websocket_connection=None, payload=None, json_deserialize=None, payload_summary=None, websocket_request_id=None, websocket_request=None
    ):
        self.websocket_connection = websocket_connection
        self.payload = payload
        self.json_deserialized_payload = json_deserialize(payload) if payload and json_deserialize else None
        self.payload_summary = payload_summary  # arbitrary dict containing parsed information (very specific for each exchange)
        self.websocket_request_id = websocket_request_id
        self.websocket_request = websocket_request

    def as_readable_dict(self):
        return {
            "websocket_connection": self.websocket_connection.as_readable_dict() if self.websocket_connection else None,
            "payload": self.payload,
            "json_deserialized_payload": self.json_deserialized_payload,
            "payload_summary": self.payload_summary,
            "websocket_request_id": self.websocket_request_id,
            "websocket_request": self.websocket_request.as_readable_dict() if self.websocket_request else None,
        }


class WebsocketRequest:
    def __init__(self, *, id=None, payload=None, json_payload=None, json_serialize=None, extra_data=None):
        self.id = id
        self.json_payload = json_payload
        if json_payload and json_serialize:
            self.payload = json_serialize(json_payload)
        else:
            self.payload = payload
        self.extra_data = extra_data

    def as_readable_dict(self):
        return self.__dict__


one_billion = 1_000_000_000


def time_point_now():
    return divmod(time.time_ns(), one_billion)


def unix_timestamp_milliseconds_now():
    return int(time.time() * 1000)


def unix_timestamp_seconds_now():
    return int(time.time())


def unix_timestamp_seconds_now_as_float():
    return time.time()


def time_point_subtract(*, time_point_1, time_point_2):
    time_point_delta = (time_point_1[0] - time_point_2[0], time_point_1[1] - time_point_2[1])
    return time_point_delta


def convert_time_point_to_unix_timestamp_seconds(*, time_point):
    return time_point[0] + time_point[1] / one_billion


def convert_time_point_delta_to_seconds(*, time_point_delta):
    return time_point_delta[0] + time_point_delta[1] / one_billion


def convert_list_to_sublists(*, input, sublist_length):
    if sublist_length:
        return [input[i * sublist_length : (i + 1) * sublist_length] for i in range((len(input) + sublist_length - 1) // sublist_length)]
    else:
        return [input]


def convert_set_to_subsets(*, input, subset_length):
    if subset_length:
        input_list = list(input)
        return [set(input_list[i * subset_length : (i + 1) * subset_length]) for i in range((len(input_list) + subset_length - 1) // subset_length)]
    else:
        return [input]


def get_base_url_from_url(*, url):
    url_splits = url.split("/")
    return f"{url_splits[0]}//{url_splits[2]}"


def convert_unix_timestamp_milliseconds_to_time_point(*, unix_timestamp_milliseconds):
    x = divmod(int(unix_timestamp_milliseconds), 1_000)
    return (x[0], x[1] * 1_000_000)


def round_to_nearest(*, input, increment, increment_as_float=None, increment_as_decimal=None):
    if increment_as_decimal is None:
        increment_as_decimal = Decimal(increment)
    return increment_as_decimal * round(round_calculate_divide(input=input, increment=increment, increment_as_float=increment_as_float))


def round_up(*, input, increment, increment_as_float=None, increment_as_decimal=None):
    if increment_as_decimal is None:
        increment_as_decimal = Decimal(increment)
    return increment_as_decimal * ceil(round_calculate_divide(input=input, increment=increment, increment_as_float=increment_as_float))


def round_down(*, input, increment, increment_as_float=None, increment_as_decimal=None):
    if increment_as_decimal is None:
        increment_as_decimal = Decimal(increment)
    return increment_as_decimal * floor(round_calculate_divide(input=input, increment=increment, increment_as_float=increment_as_float))


def round_calculate_divide(*, input, increment, increment_as_float=None):
    input_as_float = float(input)
    if increment_as_float is None:
        increment_as_float = float(increment)
    return input_as_float / increment_as_float


def create_url(*, base_url, path):
    return base_url + path


def create_path_with_query_params(*, path, query_params):
    if query_params:
        return "?".join((path, "&".join([f"{k}={urllib.parse.quote_plus(str(v))}" for k, v in sorted(dict(query_params).items())])))
    else:
        return path


def create_path_with_query_string(*, path, query_string):
    if query_string:
        return "?".join((path, query_string))
    else:
        return path


def create_url_with_query_params(*, base_url, path, query_params):
    return create_url(base_url=base_url, path=create_path_with_query_params(path=path, query_params=query_params))


def create_url_with_query_string(*, base_url, path, query_string):
    return create_url(base_url=base_url, path=create_path_with_query_string(path=path, query_string=query_string))


def remove_leading_negative_sign_in_string(*, input):
    return input[1:] if input.startswith("-") else input


def normalize_decimal_string(*, input):
    return input.rstrip("0").rstrip(".") if "." in input and input[-1] == "0" else input


def convert_decimal_to_string(*, input, normalize=False):
    output = "{0:f}".format(input)
    if normalize:
        output = normalize_decimal_string(input=output)
    return output


def convert_datetime_string_from_colon_to_hyphen(*, input):
    return input.replace(":", "-")
