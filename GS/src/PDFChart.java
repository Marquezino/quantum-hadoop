/*
 * PDFChart.java    1.0 2015/06/07
 *
 * Copyright (C) 2015 GNU General Public License
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 */


package grover;

import javax.swing.JFrame;
import javax.swing.JPanel;
import java.awt.BorderLayout;
import javax.swing.SwingUtilities;
import java.awt.Color;
import java.awt.BasicStroke;
import java.io.IOException;
import java.io.File;
import java.io.FileReader;
import java.io.BufferedReader;

import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.JFreeChart;
import org.jfree.data.time.Month;
import org.jfree.data.time.TimeSeries;
import org.jfree.data.time.TimeSeriesCollection;
import org.jfree.data.xy.XYDataset;
import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;
import org.jfree.chart.plot.PlotOrientation;

import org.jfree.chart.renderer.xy.XYLineAndShapeRenderer;
import org.jfree.chart.plot.XYPlot;
import org.jfree.chart.ChartUtilities;
import org.jfree.chart.axis.NumberAxis;


/**
 *
    This software create a PNG file with a chart of the probability distribution
    function for the Grover algorithm.
 *
 * @version
    1.0 7 Jun 2015  * @author
    David Souza  */


public class PDFChart extends JFrame {

    public PDFChart(String input, String output) {
        super("PDF Chart of the Grover Algorithm");

        JPanel chartPanel = createChartPanel(input, output);
        add(chartPanel, BorderLayout.CENTER);

        setSize(1024, 576);
        setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        setLocationRelativeTo(null);
    }

    private JPanel createChartPanel(String input, String output) {
        String chartTitle = "PDF Chart of the Grover Algorithm";
        String xAxisLabel = "Index Element";
        String yAxisLabel = "Probability";
        boolean showLegend = false;
        boolean createURL = false;
        boolean createTooltip = false;

        XYDataset dataset = createDataset(input);

        JFreeChart chart = ChartFactory.createXYLineChart(chartTitle,
                xAxisLabel, yAxisLabel, dataset, PlotOrientation.VERTICAL,
		                      showLegend, createTooltip, createURL);

		XYPlot plot = chart.getXYPlot();

        XYLineAndShapeRenderer renderer = new XYLineAndShapeRenderer();

        // sets paint color for each series
        renderer.setSeriesPaint(0, Color.CYAN);

        // sets thickness for series (using strokes)
        renderer.setSeriesStroke(0, new BasicStroke(2.0f));

        // Disable Series Shape
        renderer.setSeriesShapesVisible(0, false);

        plot.setRenderer(renderer);

        // Set range of Y axis
        NumberAxis range = (NumberAxis) plot.getRangeAxis();
        range.setRange(0.0, 1.0);

        // Setting background color for the plot
        plot.setBackgroundPaint(Color.DARK_GRAY);

        // Setting visibility and paint color for the grid lines
        plot.setRangeGridlinesVisible(false);
        plot.setRangeGridlinePaint(Color.BLACK);

        plot.setDomainGridlinesVisible(false);
        plot.setDomainGridlinePaint(Color.BLACK);

        File imageFile = new File(output + "GroversChart.png");
        int width = 1024;
        int height = 576;

        try {
            ChartUtilities.saveChartAsPNG(imageFile, chart, width, height);
        } catch (IOException ex) {
            System.err.println(ex);
        }

        return new ChartPanel(chart);
    }

    private XYDataset createDataset(String input) {
        XYSeriesCollection dataset = new XYSeriesCollection();
        XYSeries series = new XYSeries("Probability Distribution Function");
        String line;
        String[] vals;
        double x;
        double y;
        BufferedReader br;

        try {

            br = new BufferedReader(new FileReader(input));

            while ((line = br.readLine()) != null) {

                if (!(line.equals("")) && !(String.valueOf(line.charAt(0)).
                        equals(" "))) {

                    vals = line.split(",");
                    x = Double.parseDouble(vals[0]);
                    y = Double.parseDouble(vals[1]);

                    series.add(x, y);
                }
            }

            br.close();

        } catch (Exception e) {
            System.out.println(e);
        }

        dataset.addSeries(series);

        return dataset;
    }

    public static void main(String[] args) {

        final String input = args[0];
        final String output = args[1];

        SwingUtilities.invokeLater(new Runnable() {
            @Override
            public void run() {
                //new PDFChart(input, output).setVisible(true);
                new PDFChart(input, output);
            }
        });
    }

}
