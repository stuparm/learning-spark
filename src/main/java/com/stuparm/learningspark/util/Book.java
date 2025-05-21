package com.stuparm.learningspark.util;


import lombok.Getter;
import lombok.Setter;

import java.util.Date;

@Setter
@Getter

public class Book {
    int id;
    int authorId;
    String title;
    Date releaseDate;
    String link;
}
