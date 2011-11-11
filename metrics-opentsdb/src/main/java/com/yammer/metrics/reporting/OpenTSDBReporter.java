package com.yammer.metrics.reporting;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.*;
import com.yammer.metrics.core.VirtualMachineMetrics.*;
import com.yammer.metrics.util.MetricPredicate;
import com.yammer.metrics.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.lang.Thread.State;
import java.net.Socket;
import java.net.InetAddress;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import static com.yammer.metrics.core.VirtualMachineMetrics.daemonThreadCount;
import static com.yammer.metrics.core.VirtualMachineMetrics.fileDescriptorUsage;
import static com.yammer.metrics.core.VirtualMachineMetrics.garbageCollectors;
import static com.yammer.metrics.core.VirtualMachineMetrics.heapUsage;
import static com.yammer.metrics.core.VirtualMachineMetrics.memoryPoolUsage;
import static com.yammer.metrics.core.VirtualMachineMetrics.nonHeapUsage;
import static com.yammer.metrics.core.VirtualMachineMetrics.threadCount;
import static com.yammer.metrics.core.VirtualMachineMetrics.threadStatePercentages;
import static com.yammer.metrics.core.VirtualMachineMetrics.uptime;


/**
 * A simple reporter which sends out application metrics to an
 * <a href="http://opentsdb.net">OpenTSDB</a> server periodically.
 */
public class OpenTSDBReporter extends AbstractPollingReporter {
    private static final Logger LOG = LoggerFactory.getLogger(OpenTSDBReporter.class);
    private final String host;
    private final int port;
    private final String prefix;
    private final String tags;
    private final MetricPredicate predicate;
    private final Locale locale = Locale.US;
    private Writer writer;

    /**
     * Enables the opentsdb reporter to send data for the default metrics registry
     * to OpenTSDB server with the specified period.
     *
     * @param period the period between successive outputs
     * @param unit   the time unit of {@code period}
     * @param host   the host name of OpenTSDB server
     * @param port   the port number on which the OpenTSDB server is listening
     */
    public static void enable(long period, TimeUnit unit, String host, int port) {
        enable(Metrics.defaultRegistry(), period, unit, host, port);
    }

    /**
     * Enables the OpenTSDB reporter to send data for the given metrics registry
     * to OpenTSDB server with the specified period.
     *
     * @param metricsRegistry the metrics registry
     * @param period          the period between successive outputs
     * @param unit            the time unit of {@code period}
     * @param host            the host name of OpenTSDB server
     * @param port            the port number on which the OpenTSDB server is listening
     */
    public static void enable(MetricsRegistry metricsRegistry, long period, TimeUnit unit, String host, int port) {
        enable(metricsRegistry, period, unit, host, port, null);
    }

    /**
     * Enables the OpenTSDB reporter to send data to OpenTSDB server with the
     * specified period.
     *
     * @param period the period between successive outputs
     * @param unit   the time unit of {@code period}
     * @param host   the host name of OpenTSDB server
     * @param port   the port number on which the OpenTSDB server is listening
     * @param prefix the string which is prepended to all metric names
     */
    public static void enable(long period, TimeUnit unit, String host, int port, String prefix) {
        enable(Metrics.defaultRegistry(), period, unit, host, port, prefix);
    }
    public static void enable(long period, TimeUnit unit, String host, int port, String prefix, String tags) {
        enable(Metrics.defaultRegistry(), period, unit, host, port, prefix, tags, MetricPredicate.ALL);
    }

    /**
     * Enables the OpenTSDB reporter to send data to OpenTSDB server with the
     * specified period.
     *
     * @param metricsRegistry the metrics registry
     * @param period          the period between successive outputs
     * @param unit            the time unit of {@code period}
     * @param host            the host name of OpenTSDB server
     * @param port            the port number on which the OpenTSDB server is listening
     * @param prefix          the string which is prepended to all metric names
     */
    public static void enable(MetricsRegistry metricsRegistry, long period, TimeUnit unit, String host, int port, String prefix) {
        enable(metricsRegistry, period, unit, host, port, prefix, "", MetricPredicate.ALL);
    }

    /**
     * Enables the OpenTSDB reporter to send data to OpenTSDB server with the
     * specified period.
     *
     * @param metricsRegistry the metrics registry
     * @param period          the period between successive outputs
     * @param unit            the time unit of {@code period}
     * @param host            the host name of OpenTSDB server
     * @param port            the port number on which the OpenTSDB server is listening
     * @param prefix          the string which is prepended to all metric names
     * @param predicate       filters metrics to be reported
     */
    public static void enable(MetricsRegistry metricsRegistry, long period, TimeUnit unit, String host, int port, String prefix, String tags, MetricPredicate predicate) {
        try {
            final OpenTSDBReporter reporter = new OpenTSDBReporter(metricsRegistry, host, port, prefix, tags, predicate);
            reporter.start(period, unit);
        } catch (Exception e) {
            LOG.error("Error creating/starting OpenTSDB reporter:", e);
        }
    }

    /**
     * Creates a new {@link OpenTSDBReporter}.
     *
     * @param host   is OpenTSDB server
     * @param port   is port on which OpenTSDB server is running
     * @param prefix is prepended to all names reported to OpenTSDB
     * @throws IOException if there is an error connecting to the OpenTSDB server
     */
    public OpenTSDBReporter(String host, int port, String prefix) throws IOException {
        this(Metrics.defaultRegistry(), host, port, prefix, "");
    }

    /**
     * Creates a new {@link OpenTSDBReporter}.
     *
     * @param host   is OpenTSDB server
     * @param port   is port on which OpenTSDB server is running
     * @param prefix is prepended to all names reported to OpenTSDB
     * @param tags            tags to attach to metric
     * @throws IOException if there is an error connecting to the OpenTSDB server
     */
    public OpenTSDBReporter(String host, int port, String prefix, String tags) throws IOException {
        this(Metrics.defaultRegistry(), host, port, prefix, tags);
    }

    /**
     * Creates a new {@link OpenTSDBReporter}.
     *
     * @param metricsRegistry the metrics registry
     * @param host            is OpenTSDB server
     * @param port            is port on which OpenTSDB server is running
     * @param prefix          is prepended to all names reported to OpenTSDB
     * @throws IOException if there is an error connecting to the OpenTSDB server
     */
    public OpenTSDBReporter(MetricsRegistry metricsRegistry, String host, int port, String prefix) throws IOException {
        this(metricsRegistry, host, port, prefix, "", MetricPredicate.ALL);
    }

    /**
     * Creates a new {@link OpenTSDBReporter}.
     *
     * @param metricsRegistry the metrics registry
     * @param host            is OpenTSDB server
     * @param port            is port on which OpenTSDB server is running
     * @param prefix          is prepended to all names reported to OpenTSDB
     * @param tags            tags to attach to metric
     * @throws IOException if there is an error connecting to the OpenTSDB server
     */
    public OpenTSDBReporter(MetricsRegistry metricsRegistry, String host, int port, String prefix, String tags) throws IOException {
        this(metricsRegistry, host, port, prefix, tags, MetricPredicate.ALL);
    }

    /**
     * Creates a new {@link OpenTSDBReporter}.
     *
     * @param metricsRegistry the metrics registry
     * @param host            is OpenTSDB server
     * @param port            is port on which OpenTSDB server is running
     * @param prefix          is prepended to all names reported to OpenTSDB
     * @param tags            tags to attach to metric
     * @param predicate       filters metrics to be reported
     * @throws IOException if there is an error connecting to the OpenTSDB server
     */
    public OpenTSDBReporter(MetricsRegistry metricsRegistry, String host, int port, String prefix, String tags, MetricPredicate predicate) throws IOException {
        super(metricsRegistry, "OpenTSDB-reporter");
        this.host = host;
        this.port = port;
        if (prefix != null) {
            // Pre-append the "." so that we don't need to make anything conditional later.
            this.prefix = prefix + ".";
        } else {
            this.prefix = "";
        }
        this.predicate = predicate;
        // add host tag, append given tags
        String t = "";
        try {
          t += "host=" + InetAddress.getLocalHost().getHostName();
        } catch (Exception e) {
          LOG.warn("OpenTSDB reporter couldn't get host name, omitting host tag");
        }
        this.tags = t + " " + tags;
    }

    @Override
    public void run() {
        Socket socket = null;
        try {
            socket = new Socket(host, port);
            writer = new OutputStreamWriter(socket.getOutputStream());
            long epoch = System.currentTimeMillis() / 1000;
            printVmMetrics(epoch);
            printRegularMetrics(epoch);
            writer.flush();
        } catch (Exception e) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Error writing to OpenTSDB", e);
            } else {
                LOG.warn("Error writing to OpenTSDB: {}", e.getMessage());
            }
            if (writer != null) {
                try {
                    writer.flush();
                } catch (IOException e1) {
                    LOG.error("Error while flushing writer:", e1);
                }
            }
        } finally {
            if (socket != null) {
                try {
                    socket.close();
                } catch (IOException e) {
                    LOG.error("Error while closing socket:", e);
                }
            }
            writer = null;
        }
    }

    private void printRegularMetrics(long epoch) {
        for (Entry<String, Map<String, Metric>> entry : Utils.sortAndFilterMetrics(metricsRegistry.allMetrics(), this.predicate).entrySet()) {
            for (Entry<String, Metric> subEntry : entry.getValue().entrySet()) {
                final String simpleName = sanitizeName(entry.getKey() + "." + subEntry.getKey());
                final Metric metric = subEntry.getValue();
                if (metric != null) {
                    try {
                        if (metric instanceof GaugeMetric<?>) {
                            printGauge((GaugeMetric<?>) metric, simpleName, epoch);
                        } else if (metric instanceof CounterMetric) {
                            printCounter((CounterMetric) metric, simpleName, epoch);
                        } else if (metric instanceof HistogramMetric) {
                            printHistogram((HistogramMetric) metric, simpleName, epoch);
                        } else if (metric instanceof MeterMetric) {
                            printMetered((MeterMetric) metric, simpleName, epoch);
                        } else if (metric instanceof TimerMetric) {
                            printTimer((TimerMetric) metric, simpleName, epoch);
                        }
                    } catch (Exception ignored) {
                        LOG.error("Error printing regular metrics:", ignored);
                    }
                }
            }
        }
    }

    private void sendToOpenTSDB(String data) {
        try {
            writer.write(data);
        } catch (IOException e) {
            LOG.error("Error sending to OpenTSDB:", e);
        }
    }

    private String sanitizeName(String name) {
      return name.replace(' ', '-');
    }

    private void printGauge(GaugeMetric<?> gauge, String name, long epoch) {
        sendToOpenTSDB(String.format(locale, "put %s%s.%s %s %d\n", prefix, sanitizeName(name), "value", epoch, gauge.value(), this.tags));
    }

    private void printCounter(CounterMetric counter, String name, long epoch) {
        sendToOpenTSDB(String.format(locale, "put %s%s.%s %d %d\n", prefix, sanitizeName(name), "count", epoch, counter.count(), this.tags));
    }

    private void printMetered(Metered meter, String name, long epoch) {
        final String sanitizedName = sanitizeName(name);
        final StringBuilder lines = new StringBuilder();
        lines.append(String.format(locale, "put %s%s.%s %d %d %s\n",    prefix, sanitizedName, "count",        epoch, meter.count(), this.tags));
        lines.append(String.format(locale, "put %s%s.%s %d %2.2f %s\n", prefix, sanitizedName, "meanRate",     epoch, meter.meanRate(), this.tags));
        lines.append(String.format(locale, "put %s%s.%s %d %2.2f %s\n", prefix, sanitizedName, "1MinuteRate",  epoch, meter.oneMinuteRate(), this.tags));
        lines.append(String.format(locale, "put %s%s.%s %d %2.2f %s\n", prefix, sanitizedName, "5MinuteRate",  epoch, meter.fiveMinuteRate(), this.tags));
        lines.append(String.format(locale, "put %s%s.%s %d %2.2f %s\n", prefix, sanitizedName, "15MinuteRate", epoch, meter.fifteenMinuteRate(), this.tags));
        sendToOpenTSDB(lines.toString());
    }

    private void printHistogram(HistogramMetric histogram, String name, long epoch) {
        final String sanitizedName = sanitizeName(name);
        final double[] percentiles = histogram.percentiles(0.5, 0.75, 0.95, 0.98, 0.99, 0.999);
        final StringBuilder lines = new StringBuilder();
        lines.append(String.format(locale, "put %s%s.%s %d %2.2f %s\n", prefix, sanitizedName, "min",           epoch, histogram.min(), this.tags));
        lines.append(String.format(locale, "put %s%s.%s %d %2.2f %s\n", prefix, sanitizedName, "max",           epoch, histogram.max(), this.tags));
        lines.append(String.format(locale, "put %s%s.%s %d %2.2f %s\n", prefix, sanitizedName, "mean",          epoch, histogram.mean(), this.tags));
        lines.append(String.format(locale, "put %s%s.%s %d %2.2f %s\n", prefix, sanitizedName, "stddev",        epoch, histogram.stdDev(), this.tags));
        lines.append(String.format(locale, "put %s%s.%s %d %2.2f %s\n", prefix, sanitizedName, "median",        epoch, percentiles[0], this.tags));
        lines.append(String.format(locale, "put %s%s.%s %d %2.2f %s\n", prefix, sanitizedName, "75percentile",  epoch, percentiles[1], this.tags));
        lines.append(String.format(locale, "put %s%s.%s %d %2.2f %s\n", prefix, sanitizedName, "95percentile",  epoch, percentiles[2], this.tags));
        lines.append(String.format(locale, "put %s%s.%s %d %2.2f %s\n", prefix, sanitizedName, "98percentile",  epoch, percentiles[3], this.tags));
        lines.append(String.format(locale, "put %s%s.%s %d %2.2f %s\n", prefix, sanitizedName, "99percentile",  epoch, percentiles[4], this.tags));
        lines.append(String.format(locale, "put %s%s.%s %d %2.2f %s\n", prefix, sanitizedName, "999percentile", epoch, percentiles[5], this.tags));

        sendToOpenTSDB(lines.toString());
    }

    private void printTimer(TimerMetric timer, String name, long epoch) {
        printMetered(timer, name, epoch);

        final String sanitizedName = sanitizeName(name);
        final double[] percentiles = timer.percentiles(0.5, 0.75, 0.95, 0.98, 0.99, 0.999);

        final StringBuilder lines = new StringBuilder();
        lines.append(String.format(locale, "put %s%s.%s %d %2.2f %s\n", prefix, sanitizedName, "min",           epoch, timer.min(), this.tags));
        lines.append(String.format(locale, "put %s%s.%s %d %2.2f %s\n", prefix, sanitizedName, "max",           epoch, timer.max(), this.tags));
        lines.append(String.format(locale, "put %s%s.%s %d %2.2f %s\n", prefix, sanitizedName, "mean",          epoch, timer.mean(), this.tags));
        lines.append(String.format(locale, "put %s%s.%s %d %2.2f %s\n", prefix, sanitizedName, "stddev",        epoch, timer.stdDev(), this.tags));
        lines.append(String.format(locale, "put %s%s.%s %d %2.2f %s\n", prefix, sanitizedName, "median",        epoch, percentiles[0], this.tags));
        lines.append(String.format(locale, "put %s%s.%s %d %2.2f %s\n", prefix, sanitizedName, "75percentile",  epoch, percentiles[1], this.tags));
        lines.append(String.format(locale, "put %s%s.%s %d %2.2f %s\n", prefix, sanitizedName, "95percentile",  epoch, percentiles[2], this.tags));
        lines.append(String.format(locale, "put %s%s.%s %d %2.2f %s\n", prefix, sanitizedName, "98percentile",  epoch, percentiles[3], this.tags));
        lines.append(String.format(locale, "put %s%s.%s %d %2.2f %s\n", prefix, sanitizedName, "99percentile",  epoch, percentiles[4], this.tags));
        lines.append(String.format(locale, "put %s%s.%s %d %2.2f %s\n", prefix, sanitizedName, "999percentile", epoch, percentiles[5], this.tags));
        sendToOpenTSDB(lines.toString());
    }

    private void printDoubleField(String name, double value, long epoch) {
        sendToOpenTSDB(String.format(locale, "put %s%s %d %2.2f %s\n", prefix, sanitizeName(name), epoch, value, this.tags));
    }
    private void printDoubleFieldTagged(String name, double value, long epoch, String extratags) {
        sendToOpenTSDB(String.format(locale, "put %s%s %d %2.2f %s\n", prefix, sanitizeName(name), epoch, value, this.tags + " " + extratags));
    }

    private void printLongField(String name, long value, long epoch) {
        sendToOpenTSDB(String.format(locale, "put %s%s %d %d %s\n", prefix, sanitizeName(name), epoch, value, this.tags));
    }
    private void printLongFieldTagged(String name, long value, long epoch, String extratags) {
        sendToOpenTSDB(String.format(locale, "put %s%s %d %d %s\n", prefix, sanitizeName(name), epoch, value, this.tags + " " + extratags));
    }

    private void printVmMetrics(long epoch) throws IOException {
        printDoubleField("jvm.memory.heap_usage", heapUsage(), epoch);
        printDoubleField("jvm.memory.non_heap_usage", nonHeapUsage(), epoch);
        for (Entry<String, Double> pool : memoryPoolUsage().entrySet()) {
            printDoubleField("jvm.memory.memory_pool_usages." + pool.getKey(), pool.getValue(), epoch);
        }

        printDoubleField("jvm.daemon_thread_count", daemonThreadCount(), epoch);
        printDoubleField("jvm.thread_count", threadCount(), epoch);
        printDoubleField("jvm.uptime", uptime(), epoch);
        printDoubleField("jvm.fd_usage", fileDescriptorUsage(), epoch);

        for (Entry<State, Double> entry : threadStatePercentages().entrySet()) {
            printDoubleField("jvm.thread-states." + entry.getKey().toString().toLowerCase(), entry.getValue(), epoch);
        }

        for (Entry<String, GarbageCollector> entry : garbageCollectors().entrySet()) {
            printLongFieldTagged("jvm.gc.time", entry.getValue().getTime(TimeUnit.MILLISECONDS), epoch, "gc=" + entry.getKey());
            printLongFieldTagged("jvm.gc.runs", entry.getValue().getRuns(), epoch, "gc=" + entry.getKey());
        }
    }
}
