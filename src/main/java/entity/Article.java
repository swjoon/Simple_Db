package entity;

import lombok.Data;

import java.time.LocalDateTime;

@Data
public class Article {
    private Long id;
    private String title;
    private String Body;
    private LocalDateTime createdDate;
    private LocalDateTime modifiedDate;
    private boolean isBlind;
}
