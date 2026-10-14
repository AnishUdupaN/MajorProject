"""Quick smoke-test for StatusDashboard TUI fixes."""
from __future__ import annotations
import time
import sys

# Patch stdout to capture output
import io
captured = io.StringIO()
original_stdout = sys.stdout

from core import StatusDashboard

def test_message_expiry():
    """Test that messages expire based on wall-clock time, not render call count."""
    d = StatusDashboard()
    d.webserver_mode = True  # Suppress stdout rendering
    
    # Add a message with 2 second timeout
    d.add_message("Test message", timeout_seconds=2)
    
    # Rapidly call render 1000 times (simulating rapid state changes)
    for _ in range(1000):
        d.render()
    
    # Message should still be alive (we haven't waited 2 seconds)
    alive = [m for m in d.messages if m.get("expire_at", m.get("time_left", 0)) > (time.time() if "expire_at" in m else 0)]
    if not alive:
        # Check if using old time_left format
        alive = [m for m in d.messages if m.get("time_left", 0) > 0]
    
    if d.messages:
        print("✓ Messages survive rapid render calls")
    else:
        print("✗ BUG: Messages expired too quickly (render-call-rate based expiry bug still present)")
    
    # Now wait for real expiry
    time.sleep(2.1)
    d.render()
    if not d.messages:
        print("✓ Messages properly expire after wall-clock timeout")
    else:
        print("✗ BUG: Messages did not expire after 2 seconds")

def test_webserver_mode_no_stdout():
    """Test that webserver_mode suppresses stdout output."""
    d = StatusDashboard()
    d.webserver_mode = True
    
    buf = io.StringIO()
    sys.stdout = buf
    try:
        d.set_master_state("executing")
        d.update_node("192.168.1.1", "node1", "part1.mkv", "executing")
        d.render()
    finally:
        sys.stdout = original_stdout
    
    output = buf.getvalue()
    if not output:
        print("✓ Webserver mode suppresses stdout output")
    else:
        print(f"✗ BUG: Webserver mode still writes to stdout: {repr(output[:100])}")

def test_init_no_last_render_time():
    """Test that _last_render_time is removed from __init__."""
    d = StatusDashboard()
    if hasattr(d, '_last_render_time'):
        print("⚠ _last_render_time still present (may or may not be a bug depending on implementation)")
    else:
        print("✓ _last_render_time removed from init")

def test_add_message_uses_expire_at():
    """Test that add_message stores expire_at instead of time_left."""
    d = StatusDashboard()
    d.webserver_mode = True
    before = time.time()
    d.add_message("hello", timeout_seconds=5)
    after = time.time()
    
    if d.messages:
        msg = d.messages[0]
        if "expire_at" in msg:
            if before + 5 <= msg["expire_at"] <= after + 5:
                print("✓ add_message uses expire_at correctly")
            else:
                print(f"✗ BUG: expire_at value incorrect: {msg['expire_at']}")
        elif "time_left" in msg:
            if 4.9 <= msg["time_left"] <= 5.1:
                print("⚠ add_message uses time_left (old format - may still have rapid-render bug)")
            else:
                print(f"✗ BUG: time_left value incorrect: {msg['time_left']}")
        else:
            print(f"✗ BUG: Message has neither expire_at nor time_left: {msg}")
    else:
        print("✗ BUG: No messages added")

if __name__ == "__main__":
    print("=== Testing StatusDashboard TUI fixes ===")
    test_init_no_last_render_time()
    test_add_message_uses_expire_at()
    test_message_expiry()
    test_webserver_mode_no_stdout()
    print("=== Done ===")
