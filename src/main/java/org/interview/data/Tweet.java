package org.interview.data;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Date;
import lombok.Data;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class Tweet implements Comparable{
    @JsonProperty("id")
    private String id;

    @JsonProperty(access = JsonProperty.Access.WRITE_ONLY)
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern="EEE MMM dd HH:mm:ss ZZZZ yyyy", locale = "EN")
    private Date created_at;

    @JsonProperty("text")
    private String text;

    @JsonProperty("user")
    private Author author;

    public Long getCreateAt() {
        return created_at.getTime();
    }

    @Override
    public int compareTo(Object o) {
        Tweet tweet = (Tweet)o;
        if(this.author.equals(tweet.author)){
            return this.getCreateAt().compareTo(tweet.getCreateAt());
        }
        return this.author.compareTo(tweet.author);
    }

}
