package models;

import java.sql.Timestamp;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;
import org.codehaus.jackson.annotate.JsonIgnore;
import play.db.ebean.Model;

import static models.AppResult.*;


@Entity
@Table(name = "exception_fingerprinting")
public class JobsExceptionFingerPrinting extends Model {
  static final int EXCEPTION_TYPE_LENGTH_LIMIT = 16;
  static final int EXCEPTION_LOG_LENGTH_LIMIT = 5120;

  public static class TABLE {
    public static final String TABLE_NAME = "job_exception_fingerprinting";
    public static final String FLOW_EXEC_URL = "flow_exec_url";
    public static final String JOB_NAME = "job_name";
    public static final String APP_ID = "app_id";
    public static final String TASK_ID = "task_id";
    public static final String EXCEPTION_TYPE = "exception_type";
    public static final String EXCEPTION_LOG = "exception_log";
    public static final String LOG_SOURCE_INFORMATION = "log_source_information";
    public static final String IS_HELPFUL = "is_helpful";
    public static final String CLASSIFICATION = "classification";
  }

  @JsonIgnore
  @Id
  @GeneratedValue(strategy = GenerationType.IDENTITY)
  public int id;

  @Column(length = ID_LIMIT)
  public String appId;

  @Column(length = URL_LEN_LIMIT, nullable = false)
  public String flowExecUrl;

  @Column(nullable = false)
  public String jobName;

  @Column(length = EXCEPTION_LOG_LENGTH_LIMIT, nullable = false)
  public String exceptionLog;

  @Column(length = EXCEPTION_TYPE_LENGTH_LIMIT, nullable = false)
  public String exceptionType;

  @Column(nullable = true)
  public String logSourceInformation;

  @Column(length = ID_LIMIT)
  public String taskId;

  @Column(nullable = false)
  public Timestamp createdTs;

  @Column
  public Boolean isHelpful;

  @Column(nullable = true)
  public String classification;

  public static Finder<Integer, JobsExceptionFingerPrinting> find =
      new Finder<Integer, JobsExceptionFingerPrinting>(Integer.class, JobsExceptionFingerPrinting.class);

  @Override
  public void save() {
    this.createdTs = new Timestamp(System.currentTimeMillis());
    super.save();
  }
}