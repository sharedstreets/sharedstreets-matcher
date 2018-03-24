package io.sharedstreets.matcher.model;


import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.CompareToBuilder;

public class Week implements Comparable<Week> {

    private int hashCode;

    public Integer year;
    public Integer month;
    public Integer day;

    @Override
    public String toString() {
        return String.format("%04d-%02d-%02d", year, month, day);
    }

    @Override
    public int hashCode() {
        if(hashCode == 0)
            hashCode = new HashCodeBuilder(17, 31).append(year).append(month).append(day).hashCode();
        return hashCode;
    }

    @Override
    public boolean equals(Object obj) {
        Week weekObj = (Week)obj;
        EqualsBuilder builder = new EqualsBuilder().append(this.year, weekObj.year).append(this.month, weekObj.month).append(this.day, weekObj.day);

        return builder.isEquals();
    }

    @Override
    public int compareTo(Week weekObj) {

        CompareToBuilder builder = new CompareToBuilder().append(this.year, weekObj.year).append(this.month, weekObj.month).append(this.day, weekObj.day);

        return builder.toComparison();
    }
}
