from core import StatusDashboard
import time
d = StatusDashboard()
d.set_master_state("splitting file")
d.update_node("127.0.0.1", "node1", "part1", "executing")
d.set_prompt("Press [k] to kill, [w] to wait:\nChoice: ")
time.sleep(2)
d.set_prompt(None)
print("Done!")
