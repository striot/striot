module PortConfiguration where
import Network

--input ports
portNumInput1:: PortNumber
portNumInput1 = 9001::PortNumber

portNumInput2:: PortNumber
portNumInput2 = 9002::PortNumber

-- output ports (not used by this container)
portNumOutput:: PortNumber
portNumOutput = 9003::PortNumber

hostNameOutput = "haskellserver"::HostName
