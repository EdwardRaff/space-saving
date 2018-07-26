package com.github.fzakaria.topk.spacesaving;

import com.github.fzakaria.collection.DoubleLinkedList;
import com.google.common.base.Preconditions;
import java.util.Comparator;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;
import javafx.util.Pair;

/**
 * An implementation of the StreamSummaryImpl algorithm described in <i>Efficient Computation of
 * Frequent and Top-k Elements in Data Streams</i> by Metwally, Agrawal and Abbadi.
 *
 * This datstructure as outlined in the research paper is a O(1) insert for keeping track of top k frequent items in an
 * possibly infinite stream. The top k elements can be then retrieved in O(k)
 *
 * Analysis:
 * <ul>
 *     <li>Smallest counter value, min, is at most epsilon * N , where N is items seen in the stream</li>
 *     <li>True count of an uncounted item is between 0 and min</li>
 *     <li>Any item whose true true count is greater than epsilon * N is stored</li>
 * </ul>
 *
 * @see <a href="http://www.cse.ust.hk/~raywong/comp5331/References/EfficientComputationOfFrequentAndTop
 * -kElementsInDataStreams.pdf">research paper</a>
 */
@NotThreadSafe
public class StreamSummary<T> {

    /**
     * The total capacity of the data structure. Once we have seen that amount of entries through the stream,
     * new entries have error set to the minimum previous count.
     */
    private final int capacity;


    /**
     * The number of items seen in the stream
     */
    private final LongAdder n = new LongAdder();

    private Map<T, Counter<T>> cache = new HashMap<>();

    /**
     * The sorted buckets of the different counters that exist.
     * It is sorted in decreasing order.
     */
    private DoubleLinkedList<Bucket<T>> buckets = new DoubleLinkedList<>();

    /**
     * Constructor - initializes the SpaceSaving algorithm data structure with an expected error (epsilon).
     * The size of the data structure is inversely proportional to the epsilon (1/epsilon).
     * i.e. epsilon of 0.0001 will need 10000 counters
     * Any element with (frequency - error) > epsilon * N is guaranteed to be in the Stream-Summary
     * @param epsilon The acceptable error percentage (0-1]
     */
    public StreamSummary(double epsilon) {
        Preconditions.checkArgument(epsilon > 0 && epsilon <= 1);
        this.capacity = (int) Math.ceil(1.0 / epsilon);
        Bucket<T> newBucket = new Bucket<T>(0);
        DoubleLinkedList.Node<Bucket<T>> node = new DoubleLinkedList.Node<>(newBucket);
        for (int i = 0; i < capacity; i++) {
            newBucket.getChildren().add(new Counter<T>(node));
        }
        buckets.insertBeginning(node);
    }

    /**
     * Returns the max capacity for this datastructure
     */
    public int getCapacity() {
        return capacity;
    }

    /**
     * Offer an item from the stream and increment the count by one
     * @param item The item to track
     */
    public void offer(T item) {
        offer(item, 1);
    }

    /**
     * Offer an item from the stream and increment the count for this item by the specified amount.
     * @param item The item to track
     * @param increment The amount to increment
     */
    public void offer(T item, long increment) {
        Preconditions.checkState(cache.size() <= capacity, "Capacity of the cache should be bounded.");
        n.increment();
        if (cache.containsKey(item)) {
            Counter<T> counter = cache.get(item);
            incrementCounter(counter, increment);
        } 
        else 
        {
            Counter<T> minElement = buckets.getTail().getItem().getChildren().getFirst();
            long originalMinValue = minElement.getValue();
            //remove old from cache
            if (cache.containsKey(minElement.getItem())) {
                cache.remove(minElement.getItem());
            }
            cache.put(item, minElement);
            minElement.setItem(item);
            incrementCounter(minElement, increment);
            //if we aren't full on capacity yet, we don't need to add error since we have seen every item so far
            if (cache.size() <= capacity) {
                minElement.setError(originalMinValue);
            }
        }
    }
    
    protected void offer(T item, long increment, long error) 
    {
        Preconditions.checkState(cache.size() <= capacity, "Capacity of the cache should be bounded.");
        n.increment();
        if (cache.containsKey(item)) {
            Counter<T> counter = cache.get(item);
            incrementCounter(counter, increment);
        } else {
            Counter<T> minElement = buckets.getTail().getItem().getChildren().getFirst();
            long originalMinValue = minElement.getValue();
            //remove old from cache
            if (cache.containsKey(minElement.getItem())) {
                cache.remove(minElement.getItem());
            }
            cache.put(item, minElement);
            minElement.setItem(item);
            incrementCounter(minElement, increment);
            minElement.setError(error);
            //if we aren't full on capacity yet, we don't need to add error since we have seen every item so far
            if (cache.size() <= capacity) {
                minElement.setError(originalMinValue);
            }
        }
    }
    
    
    public SortedSet<MergeCounter<T>> convert()
    {
        SortedSet<MergeCounter<T>> converted = new TreeSet<>();
        buckets.stream()
                .flatMap(b -> b.getChildren().stream())
                .forEach(s-> converted.add(new MergeCounter<>(s.getItem(), s.getValue(), s.getError())));
        return converted;
    }
    
    static public <T> SortedSet<MergeCounter<T>> combine(SortedSet<MergeCounter<T>> S1, SortedSet<MergeCounter<T>> S2, int k)
    {

        long m1 = S1.last().value;
        long m2 = S2.last().value;
//        long m1 = S1.buckets.getTail().getItem().getChildren().getFirst().getValue();
//        long m2 = S2.buckets.getTail().getItem().getChildren().getFirst().getValue();
        
        TreeSet<MergeCounter<T>> merged = new TreeSet<>();
        
        S1.stream()
                .forEach(a->
                {
                    T t = a.getItem();
                    long f = a.getValue();
                    long e = a.getError();
                    SortedSet<MergeCounter<T>> tailSet = S2.tailSet(a);
                    
                    MergeCounter<T> s2_a =tailSet.isEmpty() ? null : tailSet.last();
                    if(s2_a != null && !s2_a.item.equals(t))
                        s2_a = null;
                    
                    if(s2_a != null)
                    {
                        f += s2_a.getValue();
                        e += s2_a.getError();
                    }
                    else
                    {
                        f += m2;
                        e += m2;
                    }
                    
                    merged.add(new MergeCounter<>(t, f, e));
                });
        
         S2.stream()
                .forEach(a->
                {
                    T t = a.getItem();
                    long f = a.getValue();
                    long e = a.getError();
                    MergeCounter toAdd = new MergeCounter(t, f+m1, e+m1);
                    if(!merged.contains(toAdd))
                        merged.add(toAdd);
                    
                });

         if(merged.size() > k)
         {
            MergeCounter<T> lastItem = merged.stream().limit(k+1).collect(Collectors.toList()).get(k);
            merged.tailSet(lastItem).clear();  
         }
        
        return merged;
    }

    /**
     * Returns the number of items seen in the stream so far.
     * Any element with (frequency - error) > epsilon * N is guaranteed to be in the Stream-Summary.
     * @return the number of items seen.
     */
    public long getN() {
        return n.longValue();
    }

    /**
     * Returns the top k frequent items seen in the stream summary according to the data structure.
     * The error range for an item in the top k is epsilon * N where N is the total items processed
     */
    public List<Counter<T>> getTopK(int k) {
        return buckets.stream().flatMap(b -> b.getChildren().stream())
                .limit(k).collect(Collectors.toList());
    }

    /**
     * Helper method that is outlined in the research paper above.
     * @param counter
     */
    private void incrementCounter(Counter<T> counter, long increment) 
    {
        DoubleLinkedList.Node<Bucket<T>> bucket = counter.getBucket();
        DoubleLinkedList.Node<Bucket<T>> bucketNext = bucket.getPrev();
        bucket.getItem().getChildren().remove(counter);
        counter.setValue(counter.getValue() + increment);
        
        while (bucketNext != null && counter.getValue() < bucketNext.getItem().getValue()) 
        {
            bucketNext = bucketNext.getPrev();
        }
        
        if (bucketNext != null && counter.getValue() == bucketNext.getItem().getValue()) 
        {
            bucketNext.getItem().getChildren().addLast(counter);
            counter.setBucket(bucketNext);
        } 
        else 
        {
            DoubleLinkedList.Node<Bucket<T>> bucketNew = new DoubleLinkedList.Node<>(new Bucket<T>(counter.getValue()));
            bucketNew.getItem().getChildren().addLast(counter);
            buckets.insertBefore(bucket, bucketNew);
            counter.setBucket(bucketNew);
        }

        if (bucket.getItem().getChildren().isEmpty()) {
            buckets.remove(bucket);
        }
    }

}
