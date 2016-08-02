package com.linkedin.drelephant.analysis.genericheuristics;

import com.avaje.ebean.Ebean;
import com.avaje.ebean.SqlQuery;
import com.avaje.ebean.SqlRow;
import com.avaje.ebean.SqlUpdate;
import com.linkedin.drelephant.analysis.HadoopApplicationData;
import com.linkedin.drelephant.analysis.Heuristic;
import com.linkedin.drelephant.analysis.HeuristicResult;
import com.linkedin.drelephant.analysis.HeuristicResultDetails;
import com.linkedin.drelephant.analysis.Severity;
import com.linkedin.drelephant.configurations.heuristic.HeuristicConfigurationData;
import com.linkedin.drelephant.util.Utils;

import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;


/**
 * Generic heuristic that tracks severity of some value based on percentile cutoffs
 */
public abstract class GenericPercentileHeuristic<T extends HadoopApplicationData> implements Heuristic<T> {
  private static final Logger logger = Logger.getLogger(GenericPercentileHeuristic.class);

  private static final long DAY = 24 * 60 * 60 * 1000;
  private static final int DAYS_TO_USE_FOR_PERCENTILE = 10;

  private static final String PERCENTILE_CUTOFFS_SEVERITY = "percentile_cutoffs_severity";
  private static final String PERCENTILE_UPDATE_DELAY = "percentile_update_delay";

  private double[] _severityPercentileCutoffs = {80.0, 90.0, 95.0, 99.0}; // Default value of parameters
  private long[] _severityAbsoluteCutoffs = new long[4];
  private long _lastPercentileFetch = 0;
  private long _percentileUpdateDelay = 3600 * 1000;

  private HeuristicConfigurationData _heuristicConfData;

  /**
   * @param data HadoopApplicationData for job currently being processed
   * @param details empty list which HeuristicResultDetails can be appended to to add custom details to the result
   * @return numeric value for the given data
   * @throws RuntimeException
   */
  protected abstract long getValue(T data, List<HeuristicResultDetails> details) throws RuntimeException;

  /**
   * @param data HadoopApplicationData for job currently being processed
   * @param value valueComputed by getValue
   * @return Score for given data and computed value
   * @throws RuntimeException
   */
  protected abstract int getScore(T data, long value) throws RuntimeException;

  /**
   * @param value valueComputed by getValue
   * @return value formatted to be human readable
   */
  protected abstract String format(long value);

  /**
   * @return unique identifier which will be used select data and calculate percentiles (e.g. "HDFS bytes written")
   */
  protected abstract String getPercentileHeuristicDetailName();

  /**
   * @param data HadoopApplicationData for job currently being processed
   * @return true if this heuristic should be applied for this data. false otherwise.
   */
  protected abstract boolean shouldApply(T data);

  public GenericPercentileHeuristic(HeuristicConfigurationData heuristicConfData) {
    this._heuristicConfData = heuristicConfData;

    loadParameters();
  }

  private void loadParameters() {
    Map<String, String> paramMap = _heuristicConfData.getParamMap();
    String heuristicName = _heuristicConfData.getHeuristicName();

    double[] severityPercentileCutoffs = Utils.getParam(paramMap.get(PERCENTILE_CUTOFFS_SEVERITY), _severityPercentileCutoffs.length);
    if (severityPercentileCutoffs != null) {
      _severityPercentileCutoffs = severityPercentileCutoffs;
    }
    logger.info(heuristicName + " will use " + PERCENTILE_CUTOFFS_SEVERITY + " with the following threshold settings: "
            + Arrays.toString(_severityPercentileCutoffs));


    String percentileUpdateDelayString = paramMap.get(PERCENTILE_UPDATE_DELAY);
    if (percentileUpdateDelayString != null) {
      _percentileUpdateDelay = Long.valueOf(percentileUpdateDelayString);
    }
    logger.info(heuristicName + " will use " + PERCENTILE_UPDATE_DELAY + " with the following threshold settings: "
            + _percentileUpdateDelay);
  }

  @Override
  public HeuristicConfigurationData getHeuristicConfData() {
    return _heuristicConfData;
  }

  private Severity getSeverity(long value) {
    return Severity.getSeverityAscending(
            value,
            _severityAbsoluteCutoffs[0],
            _severityAbsoluteCutoffs[1],
            _severityAbsoluteCutoffs[2],
            _severityAbsoluteCutoffs[3]);
  }

  @Override
  public HeuristicResult apply(T data) {
    if (!shouldApply(data))
      return null;
    updateCutoffs();

    long value;
    ArrayList<HeuristicResultDetails> details = new ArrayList<HeuristicResultDetails>();
    try {
      value = getValue(data, details);
    } catch (RuntimeException e) {
      logger.info(String.format("%s failed to get value: %s",
              _heuristicConfData.getHeuristicName(),
              e.getMessage()));
      return null;
    }

    int score;
    try {
      score = getScore(data, value);
    } catch (RuntimeException e) {
      logger.info(String.format("%s failed to get score: %s",
              _heuristicConfData.getHeuristicName(),
              e.getMessage()));
      return null;
    }


    HeuristicResult result = new HeuristicResult(_heuristicConfData.getClassName(),
            _heuristicConfData.getHeuristicName(),
            getSeverity(value),
            score,
            details);
    result.addResultDetail(getPercentileHeuristicDetailName(), format(value), Long.toString(value));
    result.addResultDetail(percentileInfo(_severityPercentileCutoffs[0], "low"), format(_severityAbsoluteCutoffs[0]));
    result.addResultDetail(percentileInfo(_severityPercentileCutoffs[1], "moderate"), format(_severityAbsoluteCutoffs[1]));
    result.addResultDetail(percentileInfo(_severityPercentileCutoffs[2], "severe"), format(_severityAbsoluteCutoffs[2]));
    result.addResultDetail(percentileInfo(_severityPercentileCutoffs[3], "critical"), format(_severityAbsoluteCutoffs[3]));

    return result;
  }

  protected static String percentileInfo(double percentile, String severity) {
    return String.format("Current %s%% (over last %s days) (%s severity)",
            percentile, DAYS_TO_USE_FOR_PERCENTILE, severity);
  }

  private void updateCutoffs() {
    long now = System.currentTimeMillis();
    // By putting the synchronize inside and checking the condition again we can avoid unnecessary
    // blocking since updates are infrequent
    if (_percentileUpdateDelay > 0 && now - _lastPercentileFetch > _percentileUpdateDelay) {
      synchronized (GenericPercentileHeuristic.class.getClass()) {
        if (now - _lastPercentileFetch > _percentileUpdateDelay) {
          String heuristicID = getPercentileHeuristicDetailName().toLowerCase().replace(' ', '_');

          String createViewSQL = String.format(
                  "CREATE OR REPLACE VIEW %s_view AS\n" +
                  "  SELECT D.details AS %s\n" +
                  "    FROM yarn_app_result AS R\n" +
                  "      JOIN yarn_app_heuristic_result as H\n" +
                  "        ON R.id=H.yarn_app_result_id\n" +
                  "      JOIN yarn_app_heuristic_result_details AS D\n" +
                  "        ON H.id=D.yarn_app_heuristic_result_id\n" +
                  "    WHERE R.finish_time > %s\n" +
                  "      AND D.name=\"%s\"\n" +
                  "      AND D.details IS NOT NULL\n" +
                  "    ORDER BY CAST(D.details AS UNSIGNED) ASC;\n",
                  heuristicID,
                  heuristicID,
                  now - DAY * DAYS_TO_USE_FOR_PERCENTILE, getPercentileHeuristicDetailName());
          SqlUpdate createView =  Ebean.createSqlUpdate(createViewSQL);
          createView.execute();

          String countSQL = String.format(
                  "SELECT COUNT(*) AS count\n" +
                  "  FROM %s_view;",
                  heuristicID);
          SqlQuery rowCountQuery = Ebean.createSqlQuery(countSQL);
          List<SqlRow> rowCountList = rowCountQuery.findList();
          if (rowCountList.size() == 0) {
            logger.info(String.format("'%s' doesn't have enough data to compute cutoffs. Will try again.",
                    getPercentileHeuristicDetailName()));
            return;
          } else if (rowCountList.size() > 1) {
            throw new RuntimeException(String.format("Bad SQL query for row count: got %s rows, not 1.", rowCountList.size()));
          }
          long rowCount = (Long)rowCountList.get(0).get("count");

          for (int i = 0; i < _severityPercentileCutoffs.length; i++) {

            String byteCutoffSQL = String.format(
                    "SELECT %s\n" +
                    "  FROM %s_view\n" +
                    "  LIMIT 1 OFFSET %s;",
                    heuristicID,
                    heuristicID,
                    (int)(rowCount * _severityPercentileCutoffs[i] / 100));
            SqlQuery byteCutoffQuery = Ebean.createSqlQuery(byteCutoffSQL);
            List<SqlRow> byteCutoffList = byteCutoffQuery.findList();
            if (byteCutoffList.size() == 0) {
              logger.info(String.format("'%s' doesn't have enough data to compute cutoffs. Will try again.",
                      getPercentileHeuristicDetailName()));
              return;
            } else if (byteCutoffList.size() > 1) {
              throw new RuntimeException(String.format("Bad SQL query for byte cutoff at %s%%: got %s rows, not 1.",
                      _severityPercentileCutoffs[i], byteCutoffList.size()));
            }
            _severityAbsoluteCutoffs[i] = Long.valueOf((String)byteCutoffList.get(0).get(heuristicID));
          }
          _lastPercentileFetch = now;
        }
      }
    }
  }

}
