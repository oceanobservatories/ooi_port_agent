The port agent is a process which lives in between instrument driver and instrument
and manages the physical connection to the instrument. It has all the logic internally
to manage the connection and broker data between the instrument and driver.

In addition to a connection to the instrument driver, other processes can connect to the port agent,
via publisher connections, and listen in to all communications. These connections are read only.
The port agent is also the closest code to the instrument and therefore provides a location where we
can consistently apply timestamps to the data read.
