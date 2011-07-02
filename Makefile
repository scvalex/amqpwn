FRAMING_DATA := Network/AMQP/FramingData.hs

all: framing build

framing: $(FRAMING_DATA)

build: dist/setup-config
	cabal build

clean:
	cabal clean
	rm -f $(FRAMING_DATA)

.PHONY: all framing clean

dist/setup-config: amqp.cabal
	cabal configure

$(FRAMING_DATA): Tools/Builder.hs Tools/amqp0-8.xml
	runhaskell $+ > /tmp/Framing.hs && cp /tmp/Framing.hs $@
