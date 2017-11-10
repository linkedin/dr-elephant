package models;

import java.io.Serializable;
import java.sql.Timestamp;
import javax.persistence.*;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import play.db.ebean.Model;


@Entity
@Table(name = "job_suggested_param_value")
@IdClass(JobSuggestedParamValue.PrimaryKey.class)
public class JobSuggestedParamValue extends Model{

  private static final long serialVersionUID = 1L;
  public static class TABLE {
    public static final String TABLE_NAME = "job_suggested_param_value";
    public static final String paramSetId = "paramSetId";
    public static final String paramId = "paramId";
    public static final String paramValue = "paramValue";
    public static final String createdTs = "createdTs";
    public static final String updatedTs = "updatedTs";
  }

  @EmbeddedId
  public PrimaryKey paramValuePK;
  public String paramValue;
  public Timestamp createdTs;
  public Timestamp updatedTs;

  @ManyToOne(cascade = CascadeType.ALL)
  @JoinTable(name="job_execution", joinColumns={@JoinColumn(name ="param_set_id", referencedColumnName="param_set_id")})
  public JobExecution jobExecution;

  @ManyToOne(cascade = CascadeType.ALL)
  @JoinTable(name="algo_param", joinColumns={@JoinColumn(name ="param_id", referencedColumnName="param_id")})
  public AlgoParam algoParam;

  @Embeddable
  public static class PrimaryKey implements Serializable{
      public Integer paramId;
      public Integer paramSetId;

      @Override
      public int hashCode(){
          return new HashCodeBuilder(17,37).append(paramId).append(paramSetId).toHashCode();
      }

      @Override
      public boolean equals(Object obj){
          if(obj instanceof PrimaryKey){
              if(((PrimaryKey)obj).paramId == paramId && ((PrimaryKey)obj).paramSetId == paramSetId){
                  return true;
                }
                else{
                    return false;
                }
          } else{
                return false;
          }
      }
  }


  public static Finder<PrimaryKey, JobSuggestedParamValue> find = new Finder<PrimaryKey, JobSuggestedParamValue>(PrimaryKey.class, JobSuggestedParamValue.class);


}
