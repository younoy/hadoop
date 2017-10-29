package com.yy.hadoop.adRecommend;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.StringReader;

/**
 * Created by upsmart on 17-10-29.
 */
public class A {

    public static void main(String[] args) throws IOException {

        String blogContent = "今年该换豆浆机啦，试试九阳的吧，九阳的很好用哦";

        StringReader stringReader = new StringReader(blogContent);

        IKSegmenter ikSegmenter = new IKSegmenter(stringReader,true);

        Lexeme word = null;
        while((word = ikSegmenter.next()) != null){
            String text = word.getLexemeText();
            //每篇博客的单词出现的次数
            System.out.println(text);
        }
    }
}
