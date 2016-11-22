module PortConfiguration where
import Network

--input ports (not used by this container)
portNumInput1:: PortNumber
portNumInput1 = 9001::PortNumber

portNumInput2:: PortNumber
portNumInput2 = 9002::PortNumber

-- output port
portNumOutput:: PortNumber
portNumOutput = 9002::PortNumber

hostNameOutput = "haskellserver"::HostName

