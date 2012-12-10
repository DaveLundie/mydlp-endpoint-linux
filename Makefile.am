SUBDIRS = src

cronddir = $(sysconfdir)/cron.d

install-data-hook:
	mkdir -p $(DESTDIR)$(cronddir) && \
		cp -af debian/mydlp-endpoint-linux.cron $(DESTDIR)$(cronddir)/mydlp

