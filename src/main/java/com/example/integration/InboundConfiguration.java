package com.example.integration;

import java.io.File;
import java.time.Duration;
import java.util.regex.Pattern;

import org.apache.commons.net.ftp.FTPFile;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.PollerFactory;
import org.springframework.integration.file.filters.CompositeFileListFilter;
import org.springframework.integration.file.filters.FileListFilter;
import org.springframework.integration.file.remote.session.CachingSessionFactory;
import org.springframework.integration.ftp.dsl.Ftp;
import org.springframework.integration.ftp.filters.FtpPersistentAcceptOnceFileListFilter;
import org.springframework.integration.ftp.filters.FtpRegexPatternFileListFilter;
import org.springframework.integration.ftp.session.DefaultFtpSessionFactory;
import org.springframework.integration.metadata.PropertiesPersistingMetadataStore;

@Configuration
class InboundConfiguration {

	private static final Logger log = LogManager.getLogger();

	private static final String METADATA_STORE_DIRECTORY = "ftptransferTemp";

	@Bean
	DefaultFtpSessionFactory defaultFtpSessionFactory(@Value("${ftp1.username}") String username,
			@Value("${ftp1.password}") String pw, @Value("${ftp1.host}") String host, @Value("${ftp1.port}") int port) {
		DefaultFtpSessionFactory defaultFtpSessionFactory = new DefaultFtpSessionFactory();
		defaultFtpSessionFactory.setPassword(pw);
		defaultFtpSessionFactory.setUsername(username);
		defaultFtpSessionFactory.setHost(host);
		defaultFtpSessionFactory.setPort(port);
		return defaultFtpSessionFactory;
	}

	@Bean
	IntegrationFlow inbound(DefaultFtpSessionFactory ftpSf, CachingSessionFactory<FTPFile> cacheFtpSf,
			FileListFilter<FTPFile> compositeFilter, @Value("${inbound.local.dir}") String localDirPath) {
		var localDirectory = new File(localDirPath);
		var spec = Ftp.inboundAdapter(ftpSf)
				// preserve timestamp is needed to fetch newer versions
				.preserveTimestamp(true)
//			.remoteDirectory("remote")
				.maxFetchSize(4)
				// can choose only one
//			.patternFilter("*.txt")
				.filter(compositeFilter).autoCreateLocalDirectory(true).localDirectory(localDirectory);

		return IntegrationFlow
				.from(spec, pc -> pc.poller(PollerFactory.fixedDelay(Duration.ofSeconds(1)).maxMessagesPerPoll(2)))
				.handle((file, messageHeaders) -> {
					log.info("File in outbound is : " + file + ". class is  " + file.getClass());
					messageHeaders.forEach((k, v) -> log.info("header:: " + k + ':' + v));
					try {
						Thread.sleep(10000);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					((File) file).delete();
					return null;
				}).get();
	}

	@Bean
	public CachingSessionFactory<FTPFile> cachingSessionFactory(DefaultFtpSessionFactory sessionFactory) {
		CachingSessionFactory<FTPFile> cachingSessionFactory = new CachingSessionFactory<>(sessionFactory, 5);
		return cachingSessionFactory;
	}

	@Bean
	public FileListFilter<FTPFile> compositeFilter(FtpPersistentAcceptOnceFileListFilter ftpPersistantFilter) {

		CompositeFileListFilter<FTPFile> compositeFileListFilter = new CompositeFileListFilter<FTPFile>();

		Pattern pattern = Pattern.compile(".*\\.txt$");
		FileListFilter<FTPFile> patternFilter = new FtpRegexPatternFileListFilter(pattern);
		compositeFileListFilter.addFilter(patternFilter);
		compositeFileListFilter.addFilter(ftpPersistantFilter);
		return compositeFileListFilter;
	}

	@Bean
	public FtpPersistentAcceptOnceFileListFilter ftpPersistantFilter(
			@Value("${inbound.metadata.location}") String matadatStore) {
		PropertiesPersistingMetadataStore persistingMetadataStore = new PropertiesPersistingMetadataStore();
		persistingMetadataStore.setBaseDirectory(matadatStore);
		persistingMetadataStore.afterPropertiesSet();
		FtpPersistentAcceptOnceFileListFilter filter = new FtpPersistentAcceptOnceFileListFilter(
				persistingMetadataStore, "ftp-file-prefix-");
		filter.setFlushOnUpdate(true);
		return filter;
	}

}
