/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.github.fzakaria.topk.spacesaving;

import java.util.Objects;

/**
 *
 * @author edwardraff
 */
public class MergeCounter<T> implements Comparable<MergeCounter<T>>
{
    T item;
    long value, error;

    public MergeCounter(T item, long value, long error) {
        this.item = item;
        this.value = value;
        this.error = error;
    }

    public T getItem() {
        return item;
    }

    public long getError() {
        return error;
    }

    public long getValue() {
        return value;
    }
    
    

    @Override
    public boolean equals(Object obj) 
    {
        if(obj instanceof MergeCounter)
        {
            MergeCounter<T> other = (MergeCounter<T>) obj;
            return other.item.equals(this.item);
        }
        return  false;
    }

    @Override
    public int hashCode() {
        return item.hashCode();
    }

    @Override
    public int compareTo(MergeCounter<T> o) 
    {
        //first sort by largest value
        int t1 = Long.compare(o.value, this.value);
        if(t1 == 0)//same values, go by lower error
        {
            int t2 = Long.compare(this.error, o.error);
            if(t2 == 0)//go by object reference value for sanity
            {
                if(this.item.equals(o.item))
                    return 0;//SAME
                //else, any ordering will do
                return Integer.compare(this.item.hashCode(), o.item.hashCode());
            }
            return t2;
        }
        return t1;
    }

    
    
}
