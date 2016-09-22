module PortConfiguration where
import Network

--input ports
portNumInput1:: PortNumber -- not used by this container
portNumInput1 = 9001::PortNumber

portNumInput2:: PortNumber -- not used by this container
portNumInput2 = 9002::PortNumber

-- output ports 
portNumOutput:: PortNumber
portNumOutput = 9001::PortNumber

hostNameOutput = "haskellserver"::HostName
