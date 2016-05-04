REBAR = rebar3

.PHONY: rel stagedevrel package version all tree shell

all: compile


clean:
	$(REBAR) clean


rel:
	$(REBAR) release


compile:
	$(REBAR) compile


dialyzer:
	$(REBAR) dialyzer


xref:
	$(REBAR) xref


upgrade:
	$(REBAR) upgrade 
	make tree


update:
	$(REBAR) update


tree:
	$(REBAR) tree | grep -v '=' | sed 's/ (.*//' > tree


tree-diff: tree
	git diff test -- tree


docs:
	$(REBAR) edoc


shell:
	mkdir -p data/ring
	$(REBAR) shell --config config/shell.config --name nkdist@127.0.0.1 --setcookie nk

