FRAMMING_DATA := Network/AMQP/FrammingData.hs

all: framing build

framing: $(FRAMMING_DATA)

build: dist/setup-config
	cabal build

clean:
	cabal clean
	rm -f $(FRAMMING_DATA)
	find . -name '*.o' -or -name '*.hi' -exec rm -f '{}' \;

.PHONY: all framing clean

dist/setup-config: amqp.cabal
	cabal configure

$(FRAMMING_DATA): Tools/Builder.hs Tools/amqp0-8.xml
	runhaskell $+ > /tmp/Framming.hs && cp /tmp/Framming.hs $@
