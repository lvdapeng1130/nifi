package org.apache.nifi.reporting.bo;

import java.util.concurrent.atomic.AtomicLong;

/**
 * @author: ldp
 * @time: 2020/4/10 10:30
 * @jira:
 */
public class KyCounter {

    private final String identifier;
    private final String context;
    private final String name;
    private final AtomicLong value;

    public KyCounter(final String identifier, final String context, final String name) {
        this.identifier = identifier;
        this.context = context;
        this.name = name;
        this.value = new AtomicLong(0L);
    }

    public void adjust(final long delta) {
        this.value.set(delta);
    }

    public String getName() {
        return name;
    }

    public long getValue() {
        return this.value.get();
    }

    public String getContext() {
        return context;
    }

    public String getIdentifier() {
        return identifier;
    }

    public void reset() {
        this.value.set(0);
    }

    public String toString() {
        return "Counter[identifier=" + identifier + ']';
    }

}
