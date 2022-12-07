import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CleanMapper
       extends Mapper<Object, Text, Text, Text>{
  public void map(Object key, Text value, Context context) 
                  throws IOException, InterruptedException{

    String[] data = value.toString().split(",");
    String name = new String("");
    String boro = new String("");
    String date = data[0];//date = 0, 
    String id = data[1];
    String direction = data[2];
    String ez = data[3];
    String vtoll = data[4];
    if(id.equals("21")){
      name = "Robert F. Kennedy Bridge Queens/ Bronx Plaza (TBX)";
      boro = "M/Q/Bx";
    }else if(id.equals("22")){
      name = "Robert F. Kennedy Bridge Manhattan Plaza";
      boro = "Q/Bx";
    }else if(id.contains("23")){
      name = "Bronx-Whitestone Bridge (BWB)";
      if (direction.equals("I")){
        boro = "Bx";
      }else if(direction.equals("O")){
        boro = "Q";}    
    }else if(id.equals("24")){
      name = "Henry Hudson Bridge (HHB)";
      if (direction.equals("I")){
        boro = "M";
      }else if(direction.equals("O")){
        boro = "Bx";}  
    }else if(id.equals("25")){
      name = "Marine Parkway-Gil Hodges Memorial Bridge (MPB)";
      if (direction.equals("I")){
        boro = "ROCK";
      }else if(direction.equals("O")){
        boro = "B";}  
    }else if(id.equals("26")){
      name = "Cross Bay Veterans Memorial Bridge (CBB)";
      if (direction.equals("I")){
        boro = "ROCK";
      }else if(direction.equals("O")){
        boro = "Q";}  
    }else if(id.equals("27")){
      name = "Queens Midtown Tunnel (QMT)";
      if (direction.equals("I")){
        boro = "M";
      }else if(direction.equals("O")){
        boro = "Q";}  
    }else if(id.equals("28")){
      name = "Hugh L. Carey Tunnel (HCT)";
      if (direction.equals("I")){
        boro = "M";
      }else if(direction.equals("O")){
        boro = "B";}  
    }else if(id.equals("29")){
      name = "Throgs Neck Bridge (TNB)";
      if (direction.equals("I")){
        boro = "Bx";
      }else if(direction.equals("O")){
        boro = "Q";}  
    }else if(id.equals("30")){
      name = "Verrazano-Narrows Bridge (VNB)";
      if (direction.equals("I")){
        boro = "B";
      }else if(direction.equals("O")){
        boro = "SI";}  
    }
    //date = 0, 
    //plaza id=1, 
    //direction=2, 
    //ezpass=3, vtoll=4
    if (date.contains("2020")|date.contains("2019")|date.contains("2021") &&(id != null) && (direction!=null) && (data[3]!=null) && (data[4]!=null)){        
      //date, plaza id, name, boro, ez, vtoll
      Integer count = Integer.parseInt(vtoll)+Integer.parseInt(ez);
      String output = date +","+id +","+ name +"," + boro + "," + count;

      context.write(new Text(output),new Text(""));
    }
  }
}






