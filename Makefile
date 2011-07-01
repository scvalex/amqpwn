all: framing

framing: Network/AMQP/Generated.hs

clean:
	rm -f Network/AMQP/Generated.hs

.PHONY: all framing clean

Network/AMQP/Generated.hs: Tools/Builder.hs Tools/amqp0-8.xml Tools/Generated.hs.in
	runhaskell $+ > $@
