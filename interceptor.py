import os
from mitmproxy import http, ctx

# Function to log WebSocket messages
def websocket_message(flow: http.HTTPFlow) -> None:
    # Check if the flow is for a WebSocket connection
    if flow.websocket:
        # Get WebSocket URL
        ws_url = flow.request.pretty_url

        # Decode message content assuming UTF-8
        message_content = flow.websocket.messages[-1].content.decode('utf-8')

        # Log the response directly to a log file
        log_response(ws_url, message_content)

def log_response(ws_url: str, message_content: str) -> None:
    try:
        # Get the directory of the current script
        script_dir = os.path.dirname(os.path.abspath(__file__))

        # Directory for logs
        log_dir = os.path.join(script_dir, 'logs')

        # Ensure directory exists
        os.makedirs(log_dir, exist_ok=True)

        # Log file path
        log_file = os.path.join(log_dir, 'websocket_responses.log')

        # Write to log file
        with open(log_file, 'a', encoding='utf-8') as f:
            f.write(f"WebSocket URL: {ws_url}\n")
            f.write(f"Response:\n{message_content}\n\n")

    except Exception as e:
        ctx.log.error(f"Failed to log WebSocket response: {e}")

# Hook to save logged responses when mitmproxy exits
def done():
    ctx.log.info("Script execution completed. WebSocket responses logged to 'websocket_responses.log'.")

# Register the done() function as a callback to execute when mitmproxy exits
addons = [
    done()
]
