###
###    Copyright (C) 2010 Huseyin Kerem Cevahir <kerem@medra.com.tr>
###
###--------------------------------------------------------------------------
###    This file is part of MyDLP.
###
###    MyDLP is free software: you can redistribute it and/or modify
###    it under the terms of the GNU General Public License as published by
###    the Free Software Foundation, either version 3 of the License, or
###    (at your option) any later version.
###
###    MyDLP is distributed in the hope that it will be useful,
###    but WITHOUT ANY WARRANTY; without even the implied warranty of
###    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
###    GNU General Public License for more details.
###
###    You should have received a copy of the GNU General Public License
###    along with MyDLP.  If not, see <http://www.gnu.org/licenses/>.
###--------------------------------------------------------------------------

mydlpconfdir = $(sysconfdir)/mydlp

mydlpconf_DATA = \
	mydlp.app \
	mydlp-sasl.config \
	mydlp.conf \
	denied_page.html

dist_erl_DATA = \
	dynamic_compile.erl \
	mydlp_acl.erl \
	mydlp_container.erl \
	mydlp_fsm.erl \
	mydlp_item_push.erl \
	mydlp_logger.erl \
	mydlp_logger_file.erl \
	mydlp_sync.erl \
	mydlp_acceptor.erl \
	mydlp_sup.erl \
	mydlp_matchers.erl \
	mydlp_mnesia.erl \
	mydlp_tc.erl \
	mydlp_api.erl \
	mydlp_app.erl \
	mydlp_workdir.erl \
	mydlp_worker_sup.erl \
	mydlp_pool.erl \
	mydlp_seap_fsm.erl \
	mydlp_dynamic.erl \
	mydlp_regex.erl \
	mydlp_pdm.erl \
	mydlp_nlp.erl \
	mydlp_mc.erl \
	mydlp_discover_fs.erl \
	mydlp_spool.erl \
	mime_util.erl

dist_hrl_DATA = \
	mydlp.hrl \
	mydlp_acl.hrl \
	mydlp_big_binary.hrl \
	mydlp_logger.hrl

beam_DATA = \
	$(mydlp_erl:.erl=.beam) \
	mydlp.app

boot_DATA = \
	mydlp.boot \
	mydlp.script

dist_noinst_DATA = \
	overview.edoc.in

html_DATA = \
	$(dist_erl_DATA:.erl=.html) \
	erlang.png \
	edoc-info \
	stylesheet.css \
	index.html \
	overview-summary.html \
	modules-frame.html \
	packages-frame.html

mydlp_edoc = \
	mydlp_license.html \
	$(html_DATA)

erldir = $(ERLANG_INSTALL_LIB_DIR_mydlp)/src
hrldir = $(ERLANG_INSTALL_LIB_DIR_mydlp)/include
beamdir = $(ERLANG_INSTALL_LIB_DIR_mydlp)/ebin
privdir = $(ERLANG_INSTALL_LIB_DIR_mydlp)/priv
htmldir = $(ERLANG_INSTALL_LIB_DIR_mydlp)/doc
bootdir = $(libdir)/erlang/bin
ERLCFLAGS += -W -I ../thrift/gen-erl -D__MYDLP_ENDPOINT -D__PLATFORM_LINUX 

$(beam_DATA): $(dist_hrl_DATA)
$(boot_DATA): $(beam_DATA)

.erl.beam:
	@echo "Compiling $<..."
	@$(ERLC) $(ERLCFLAGS) $<

.rel.boot:
	@echo "Generating boot scripts..."
	@$(ERLC) -I. $<

.rel.script:
	@echo "Generating boot scripts..."
	@$(ERLC) -I. $<

$(mydlp_edoc): $(mydlp_erl) overview.edoc 
	@echo "Generating documentation..."
	@$(ERL) -noshell -run edoc_run application mydlp '"."' '[{dir,"."}]'

check:
	@echo "Running unit tests..."
	@$(ERL) -eval "eunit:test({application,mydlp})" -noshell -s init stop

uniq-console:
	@echo "Spawning console..."
	@$(ERL) -pa ../thrift/gen-erl -boot start_sasl -sname console-$$$$ 

run: $(beam_DATA) $(boot_DATA)
	@echo "Running application..."
	@MYDLP_CONF=mydlp.conf MYDLP_MNESIA_DIR=Mnesia.devel $(ERL) -pa ../thrift/gen-erl -sname console -boot mydlp 

run-production: $(beam_DATA) $(boot_DATA)
	@echo "Running application..."
	@MYDLP_CONF=mydlp.conf MYDLP_MNESIA_DIR=Mnesia.devel $(ERL) -pa ../thrift/gen-erl -sname console -config mydlp-sasl -boot mydlp 

clean-local:
	rm -rf $(beam_DATA) $(boot_DATA) $(mydlp_edoc) mydlp.rel overview.edoc Mnesia.*

