/*
 * Copyright (c) 2010-2013 the original author or authors
 * 
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 * 
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
 * LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 * OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
 * WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 * 
 */
package org.jmxtrans.samples.graphite;

import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartUtilities;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.chart.renderer.xy.XYSplineRenderer;
import org.jfree.data.time.*;
import org.joda.time.DateTime;
import org.joda.time.DateTimeConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.net.Socket;
import java.text.SimpleDateFormat;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * @author <a href="mailto:cleclerc@xebia.fr">Cyrille Le Clerc</a>
 */
public class LoadSimulator {

    protected final Logger logger = LoggerFactory.getLogger(getClass());
    private final Random random = new Random();
    private int graphitePort = 2003;
    private String graphiteMetricPrefix = "servers.cloudbees.";
    private String graphiteHost = "localhost";
    private int[] weeklyDistribution = new int[8];
    private int[] hourlyDistribution = new int[24];

    public LoadSimulator() {
        weeklyDistribution[DateTimeConstants.MONDAY] = 2;
        weeklyDistribution[DateTimeConstants.TUESDAY] = 2;
        weeklyDistribution[DateTimeConstants.WEDNESDAY] = 6;
        weeklyDistribution[DateTimeConstants.THURSDAY] = 8;
        weeklyDistribution[DateTimeConstants.FRIDAY] = 10;
        weeklyDistribution[DateTimeConstants.SATURDAY] = 10;
        weeklyDistribution[DateTimeConstants.SUNDAY] = 8;

        hourlyDistribution[0] = 8;
        hourlyDistribution[1] = 8;
        hourlyDistribution[2] = 5;
        hourlyDistribution[3] = 5;
        hourlyDistribution[4] = 3;
        hourlyDistribution[5] = 3;
        hourlyDistribution[6] = 3;
        hourlyDistribution[7] = 3;
        hourlyDistribution[8] = 3;
        hourlyDistribution[9] = 3;
        hourlyDistribution[10] = 3;
        hourlyDistribution[11] = 8;
        hourlyDistribution[12] = 8;
        hourlyDistribution[13] = 8;
        hourlyDistribution[14] = 8;
        hourlyDistribution[15] = 6;
        hourlyDistribution[16] = 6;
        hourlyDistribution[17] = 6;
        hourlyDistribution[18] = 8;
        hourlyDistribution[19] = 8;
        hourlyDistribution[20] = 10;
        hourlyDistribution[21] = 10;
        hourlyDistribution[22] = 10;
        hourlyDistribution[23] = 10;

    }

    public static void main(String[] args) throws Exception {
        LoadSimulator loadSimulator = new LoadSimulator();
        loadSimulator.generateLoad();
    }

    public void generateLoad() throws Exception {

        TimeSeries timeSeries = new TimeSeries("shopping-cart.raw");

        DateTime now = new DateTime();
        DateTime twelveDaysAgo = now.minusDays(12);

        int nowAsWeekOfYear = now.getWeekOfWeekyear();

        DateTime date = now.minusDays(15);

        int integratedValue = 0;

        MathContext mathContext = new MathContext(1, RoundingMode.CEILING);

        int randomFactor = 0;

        while (date.isBefore(now)) {
            if (timeSeries.getItemCount() % 120 == 0) {
                randomFactor = 10 + random.nextInt(2);
            }
            int weekGrowthFactor = 6 - (nowAsWeekOfYear - date.getWeekOfWeekyear());
            int value =
                    new BigDecimal(randomFactor) // random factor
                            .multiply(new BigDecimal(10)) // go to cents of USD
                            .multiply(new BigDecimal(weekGrowthFactor))
                            .multiply(new BigDecimal(hourlyDistribution[date.getHourOfDay()]))
                            .multiply(new BigDecimal(weeklyDistribution[date.getDayOfWeek()]))
                            .divide(new BigDecimal(20), mathContext).intValue(); // split hourly value in minutes


            logger.debug(date + "\t" + value);
            integratedValue += value;
            for (int i1 = 0; i1 < 3; i1++) {
                timeSeries.add(new Minute(date.toDate()), integratedValue);
                date = date.plusMinutes(1);
            }
        }

        TimeSeries ordersPriceInCentsTimeSeries = MovingAverage.createMovingAverage(timeSeries, "shopping-cart.OrdersPriceInCents", 60 * 7, 0);


        TimeSeries ordersPriceInCentsSrv1TimeSeries = new TimeSeries("srv1.shopping-cart.OrdersPriceInCents");
        TimeSeries ordersPriceInCentsSrv2TimeSeries = new TimeSeries("srv2.shopping-cart.OrdersPriceInCents");
        int resetValue2ToZeroOffset = 0; // reset value 2 after 3 days of metrics
        for (int i = 0; i < ordersPriceInCentsTimeSeries.getItemCount(); i++) {
            TimeSeriesDataItem dataItem = ordersPriceInCentsTimeSeries.getDataItem(i);
            int value = dataItem.getValue().intValue();
            // value1 is 5% higher to value2 due to a 'weirdness' in the load balancing
            int value1 = Math.min((int) (value * 1.05 / 2), value);

            {
                // simulate srv2 restart
                DateTime currentDate = new DateTime(dataItem.getPeriod().getStart());
                boolean shouldResetValue2 = resetValue2ToZeroOffset == 0 && currentDate.getDayOfYear() == twelveDaysAgo.getDayOfYear();
                if (shouldResetValue2) {
                    resetValue2ToZeroOffset = value - value1;
                    System.out.println("reset value2 of " + resetValue2ToZeroOffset + " at " + currentDate);
                }
            }

            int value2 = value - value1 - resetValue2ToZeroOffset;
            // System.out.println("value=" + value + ", value1=" + value1 + ", value2=" + value2);
            ordersPriceInCentsSrv1TimeSeries.add(dataItem.getPeriod(), value1);
            ordersPriceInCentsSrv2TimeSeries.add(dataItem.getPeriod(), value2);
        }


        TimeSeries orderItemsCountTimeSeries = new TimeSeries("shopping-cart.OrderItemsCount");
        TimeSeries orderItemsCountSrv1TimeSeries = new TimeSeries("srv1.shopping-cart.OrderItemsCount");
        TimeSeries orderItemsCountSrv2TimeSeries = new TimeSeries("srv2.shopping-cart.OrderItemsCount");

        for (int i = 0; i < ordersPriceInCentsTimeSeries.getItemCount(); i++) {
            RegularTimePeriod period = ordersPriceInCentsTimeSeries.getDataItem(i).getPeriod();
            int ordersPriceInCents1 = ordersPriceInCentsSrv1TimeSeries.getDataItem(i).getValue().intValue();
            int ordersPriceInCents2 = ordersPriceInCentsSrv2TimeSeries.getDataItem(i).getValue().intValue();

            int value1 = ordersPriceInCents1 / 600;
            int value2 = ordersPriceInCents2 / 600;

            orderItemsCountTimeSeries.add(period, ordersPriceInCents1 + ordersPriceInCents2);
            orderItemsCountSrv1TimeSeries.add(period, value1);
            orderItemsCountSrv2TimeSeries.add(period, value2);

        }

        exportMetrics(
                ordersPriceInCentsTimeSeries, ordersPriceInCentsSrv1TimeSeries, ordersPriceInCentsSrv2TimeSeries, ordersPriceInCentsTimeSeries,
                orderItemsCountTimeSeries, orderItemsCountSrv1TimeSeries, orderItemsCountSrv2TimeSeries);


    }

    public void exportMetrics(TimeSeries... timeSeries) throws IOException {
        for (TimeSeries ts : timeSeries) {
            exportMetrics(ts);
        }
    }

    public void exportMetrics(TimeSeries timeSeries) throws IOException {

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");

        if (logger.isDebugEnabled()) {
            JFreeChart chart = ChartFactory.createXYLineChart(
                    "Purchase", "date", "Amount", new TimeSeriesCollection(timeSeries), PlotOrientation.VERTICAL, true, true, false);
            chart.getXYPlot().setRenderer(new XYSplineRenderer(60));

            File file = new File("/tmp/" + timeSeries.getKey() + ".png");
            ChartUtilities.saveChartAsPNG(file, chart, 1200, 800);
            logger.info("Exported " + file.getAbsolutePath());
        }


        Socket socket = new Socket(graphiteHost, graphitePort);
        PrintWriter writer = new PrintWriter(socket.getOutputStream());

        for (int i = 0; i < timeSeries.getItemCount(); i++) {
            logger.debug(sdf.format(timeSeries.getDataItem(i).getPeriod().getStart()) + "\t" + timeSeries.getDataItem(i).getValue().intValue());
            long time = TimeUnit.SECONDS.convert(timeSeries.getDataItem(i).getPeriod().getStart().getTime(), TimeUnit.MILLISECONDS);
            Number value = timeSeries.getDataItem(i).getValue();

            writer.println(graphiteMetricPrefix + timeSeries.getKey() + " " + value + " " + time);
        }
        writer.flush();
        writer.close();
        logger.debug("Wrote " + timeSeries.getItemCount() + " items");
    }
}
