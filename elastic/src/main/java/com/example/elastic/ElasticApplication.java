package com.example.elastic;

import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.aot.hint.MemberCategory;
import org.springframework.aot.hint.RuntimeHints;
import org.springframework.aot.hint.RuntimeHintsRegistrar;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ImportRuntimeHints;
import org.springframework.data.annotation.Id;
import org.springframework.data.elasticsearch.annotations.DateFormat;
import org.springframework.data.elasticsearch.annotations.Field;
import org.springframework.data.elasticsearch.annotations.FieldType;
import org.springframework.data.elasticsearch.client.elc.NativeQuery;
import org.springframework.data.elasticsearch.core.ElasticsearchOperations;
import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;
import org.springframework.stereotype.Service;
import org.springframework.util.Assert;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

@SpringBootApplication
@ImportRuntimeHints(ElasticApplication.Hints.class)
public class ElasticApplication {

    static class Hints implements RuntimeHintsRegistrar {

        @Override
        public void registerHints(RuntimeHints hints, @Nullable ClassLoader classLoader) {
            hints.reflection().registerType(Podcast.class, MemberCategory.values());
        }
    }

    public static void main(String[] args) {
        SpringApplication.run(ElasticApplication.class, args);
    }

    private void debug(Document document) {
        var map = Map.of(
                "title", defaultText(document.title()),
                "id", document.id(),
                "description", defaultText(document.description()),
                "transcript", defaultText(document.transcript())
        );
        line();
        for (var e : map.entrySet())
            row(e.getKey(), e.getValue());

    }

    private void line() {
        IO.println("=".repeat(labelW + gutterW + textW));
    }

    private final int labelW = 15;
    private final int gutterW = 2;
    private final int textW = 120;

    private void row(String label, String text) {
        IO.println("%s%s%s".formatted(indent(label, labelW), " ".repeat(gutterW),
                width(text, textW)));
    }

    private void debug(Podcast podcast) {
        var map = Map.of(
                "id", Long.toString(podcast.id()),
                "description", defaultText(podcast.description()),
                "title", defaultText(podcast.title()),
                "transcript", defaultText(podcast.transcript()));
        this.line();
        for (var e : map.entrySet())
            row(e.getKey(), e.getValue());
    }

    private final Logger log = LoggerFactory.getLogger(getClass());

    private void ingest(EpisodeRepository episodeRepository, DocumentRepository documentRepository) {
        var episodes = episodeRepository.all();
        if (documentRepository.count() != episodes.size()) {
            episodeRepository.all().forEach(podcast -> {
                this.debug(podcast);
                var document = new Document(Long.toString(podcast.id()),
                        podcast.title(), Instant.now(), podcast.description(), podcast.transcript());
                documentRepository.save(document);
            });
        } //
        else {
            log.info("we've already ingested all the records");
        }
    }

    @Bean
    ApplicationRunner runner(
            ElasticsearchSearch elasticsearchSearch,
            DataSearch dataSearch,
            EpisodeRepository episodeRepository,
            DocumentRepository documentRepository) {
        return _ -> {
            this.ingest(episodeRepository, documentRepository);
            this.search(elasticsearchSearch, "vaadin");
        };
    }

    private void search(ElasticsearchSearch search, String q) {
        var all = search.find(q);
        IO.println("there are " + all.size() + " results for the search term [" + q + "]");
        all.forEach(this::debug);
    }

    static String indent(String colName, int w) {
        if (colName == null || colName.isEmpty()) return colName;
        var delta = w - colName.length();
        return " ".repeat(Math.max(0, delta)) + colName;
    }

    static String defaultText(String i) {
        return i == null ? "" : i;
    }

    static String sanitize(String input) {
        var n = new StringBuilder();
        for (var i = 0; i < input.length(); i++) {
            var theChar = input.charAt(i);
            if (Character.isAlphabetic(theChar) || Character.isLetterOrDigit(theChar) || theChar == ' ')
                n.append(theChar);
        }
        return n.toString();
    }

    static String width(String input, int maxWidth) {
        if (input == null || input.isEmpty()) {
            return input;
        }

        // If the string already fits, return as-is
        if (input.length() <= maxWidth) {
            return input;
        }

        input = sanitize(input);

        // Minimum width needed for abbreviation (at least "a...b")
        if (maxWidth < 5) {
            // Too short to abbreviate meaningfully, just truncate
            return input.substring(0, Math.min(input.length(), maxWidth));
        }

        // Calculate how many characters we can show (excluding "...")
        var availableChars = maxWidth - 3; // 3 for "..."

        // Split available characters between start and end
        var startChars = availableChars / 2;
        var endChars = availableChars - startChars; // This handles odd numbers

        // Extract start and end portions
        var start = input.substring(0, startChars);
        var end = input.substring(input.length() - endChars);

        return start + "..." + end;
    }
}


@Repository
class EpisodeRepository {

    private final JdbcClient db;

    EpisodeRepository(JdbcClient db) {
        this.db = db;
    }

    Collection<Podcast> all() {
        return this.db
                .sql("""
                        SELECT 
                            pe.id,
                            pe.title,
                            pe.description,
                            STRING_AGG(t.transcript, ' ' ORDER BY pes.sequence_number) as transcript
                        FROM podcast_episode pe
                        INNER JOIN podcast p ON p.id = pe.podcast_id
                        LEFT JOIN podcast_episode_segment pes ON pes.podcast_episode_id = pe.id
                        LEFT JOIN transcript t ON t.id = pes.segment_audio_managed_file_id
                        WHERE pe.complete = true
                        GROUP BY pe.id, pe.title, pe.description
                        ORDER BY pe.id
                        """)
                .query(Podcast.class)
                .list();
    }
}

record Podcast(String title, String description, String transcript, long id) {
}


interface Search {
    Collection<Document> find(String query);
}

@Service
class DataSearch implements Search {

    private final DocumentRepository repository;

    DataSearch(DocumentRepository repository) {
        this.repository = repository;
    }

    @Override
    public Collection<Document> find(String query) {
        Assert.hasText(query, "the query must not be null or empty");
        return this.repository
                .findByTitleOrDescriptionOrTranscript(
                        query, query, query);
    }
}

@Service
class ElasticsearchSearch implements Search {

    private final ElasticsearchOperations ops;

    ElasticsearchSearch(ElasticsearchOperations elasticsearchOperations) {
        this.ops = elasticsearchOperations;
    }

    @Override
    public Collection<Document> find(String shouldContain) {

        var nativeQuery = NativeQuery
                .builder()
                .withQuery(q -> q
                        .bool(b -> {
                            // Must match this term
                            b.must(m -> m
                                    .multiMatch(mm -> mm
                                            .query(shouldContain)
                                            .fields("title^2", "description^2", "transcript")
                                            .fuzziness("AUTO")));

                            // Should match (boosts relevance if present)
                            if (shouldContain != null && !shouldContain.isBlank()) {
                                b.should(s -> s
                                        .match(mt -> mt
                                                .field("transcript")
                                                .query(shouldContain)));
                            }
                            return b;
                        }))
                .withMaxResults(1000)
                .build();
        var results = new ArrayList<Document>();
        this.ops.search(nativeQuery, Document.class)
                .forEach(sh -> results.add(sh.getContent()));
        return results;
    }
}

interface DocumentRepository extends ElasticsearchRepository<Document, String> {

    Collection<Document> findByTitleOrDescriptionOrTranscript(
            String title, String description, String transcript);
}

@org.springframework.data.elasticsearch.annotations.Document(
        createIndex = false,
        indexName = "documents")
record Document(
        @Id
        String id,

        @Field(type = FieldType.Text, analyzer = "english")
        String title,

        @Field(type = FieldType.Date, format = DateFormat.epoch_millis)
        Instant when,

        @Field(type = FieldType.Text, analyzer = "english")
        String description,

        @Field(type = FieldType.Text, analyzer = "english")
        String transcript
) {
}