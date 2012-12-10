SUBDIRS = src

cronddir = $(sysconfdir)/cron.d

install-data-hook:
	mkdir -p $(DESTDIR)$(cronddir) && \
		cp -af debian/mydlp.cron $(DESTDIR)$(cronddir)/mydlp

