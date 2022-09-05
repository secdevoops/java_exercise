package org.interview.data;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Date;
import java.util.Objects;
import lombok.Data;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class Author implements Comparable{

    @JsonProperty("id")
    private String id;

    @JsonProperty(access = JsonProperty.Access.WRITE_ONLY)
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern="EEE MMM dd HH:mm:ss ZZZZZ yyyy", locale = "EN")
    private Date created_at;

    @JsonProperty("name")
    private String name;

    @JsonProperty("screen_name")
    private String screenName;

    public Long getCreateAt() {
        return created_at.getTime();
    }

    @Override
    public int compareTo(Object o) {
        Author author = (Author)o;
        return this.getCreateAt().compareTo(author.getCreateAt());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Author author = (Author) o;
        return id.equals(author.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }
}
