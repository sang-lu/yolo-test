import sys
import threading
import queue
import time
import os
import binascii
import requests
import json

CENTRIFUGE_URL = "wss://rt.aixblock.io/centrifugo/connection/websocket"
CENTRIFUGE_SECRET = os.environ.get("CENTRIFUGE_SECRET", "d0a70289-9806-41f6-be6d-f4de5fe298fb")
CENTRIFUGE_TOPIC_PREFIX = os.environ.get("CENTRIFUGE_TOPIC_PREFIX", "")
HOST_NAME = os.getenv("HOST_NAME", "https://app.aixblock.io")
TOKEN = os.getenv("TOKEN", "3cf7af3b5e87cd8674c548689ffa53561a2a8388")

if CENTRIFUGE_TOPIC_PREFIX == "":
    if HOST_NAME == "":
        CENTRIFUGE_TOPIC_PREFIX = "prefix/"
    else:
        hostname_hash = str(hex(binascii.crc32(HOST_NAME.encode("utf-8"))))
        CENTRIFUGE_TOPIC_PREFIX = (hostname_hash[0:16] if len(hostname_hash) > 16 else hostname_hash) + "/"


CENTRIFUGE_API = os.environ.get("CENTRIFUGE_API", "https://rt.aixblock.io/centrifugo/api")
CENTRIFUGE_API_KEY = os.environ.get("CENTRIFUGE_API_KEY", "ee5f81f5-0f68-48c7-a8e3-d790d92e0fd4")

def publish_message(channel, data, prefix=True, **kwargs):
    def p():
        topic = channel
        if prefix:
            topic = CENTRIFUGE_TOPIC_PREFIX + channel
        r = requests.post(
            CENTRIFUGE_API + "/publish",
            headers={"X-API-Key": CENTRIFUGE_API_KEY},
            data=json.dumps({"channel": topic, "data": data}),
        )


    t = threading.Thread(target=p, args=list())
    t.start()
    return t

class StreamLogger:
    def __init__(self, original_stream, log_queue):
        self.original_stream = original_stream  # Original stream (stdout or stderr)
        self.log_queue = log_queue  # Queue used to forward log lines to another thread

    def write(self, message):
        if message.strip():  # Only log non-empty lines
            self.original_stream.write(message)  # Write to terminal
            self.original_stream.flush()  # Ensure it's written immediately
            self.log_queue.put(message)  # Push the log line onto the queue

    def flush(self):
        self.original_stream.flush()  # Delegate to the original stream's flush

    def isatty(self):
        return self.original_stream.isatty()  # Preserve isatty() behavior

    def fileno(self):
        return self.original_stream.fileno()  # Keep other libraries able to use this

def log_worker(log_queue, channel):
    """Background thread that processes queued log lines."""
    with open("real_time_logs.log", "w") as log_file:
        while True:
            try:
                message = log_queue.get()
                if message is None:
                    break  # Stop the thread when the sentinel value is received
                log_file.write(message)  # Write the log line to file
                log_file.flush()  # Ensure it's written immediately
                publish_message(channel=channel, data={"log": message}, prefix=False)
            except Exception as e:
                print(f"Logging error: {e}", file=sys.__stderr__)

def start_queue(channel):
    # Create the queue and background thread
    log_queue = queue.Queue()
    logging_thread = threading.Thread(target=log_worker, args=(log_queue, channel))
    logging_thread.daemon = True  # Make sure the thread stops when the program exits
    logging_thread.start()
    return log_queue, logging_thread

def write_log(log_queue):
    # Redirect sys.stdout and sys.stderr
    sys.stdout = StreamLogger(sys.stdout, log_queue)
    sys.stderr = StreamLogger(sys.stderr, log_queue)

# Example usage
# try:
#     print("Starting training...")  # Will be written to both terminal and file
#     time.sleep(2)  # Simulate training
#     print("Training is in progress...")
#     time.sleep(2)
#     print("Training completed!")
# except Exception as e:
#     print(f"An error occurred: {e}")

def stop_log(log_queue, logging_thread):
# Stop the logging thread
    log_queue.put(None)
    logging_thread.join()

    # Restore sys.stdout and sys.stderr
    sys.stdout = sys.__stdout__
    sys.stderr = sys.__stderr__


# def reset_log():
#     # Restore sys.stdout and sys.stderr
#     sys.stdout = sys.__stdout__
#     sys.stderr = sys.__stderr__

# print("Logging finished. Log saved to 'real_time_logs.log'")
