package com.example.springbatch;

import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.NonTransientResourceException;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class SimpleItemReader implements ItemReader<String> {
    private List<String> dataset = new ArrayList<>();

    private Iterator<String> iterator;

    public SimpleItemReader(){
        this.dataset.add("1");
        this.dataset.add("2");
        this.dataset.add("3");
        this.dataset.add("4");
        this.dataset.add("5");
        this.dataset.add("6");
        this.dataset.add("7");
        this.iterator = this.dataset.listIterator();
    }
    @Override
    public String read() throws Exception, UnexpectedInputException, ParseException, NonTransientResourceException {
        return iterator.hasNext()?iterator.next():null;
    }
}
