package Bean;

import java.io.Serializable;

public class ItemViewCount implements Serializable {
    public long itemId;
    public long windowEnd;
    public long viewCount;

    public ItemViewCount(){

    }
    public ItemViewCount(long itemId, long windowEnd, long viewCount){
        this.itemId = itemId;
        this.viewCount = viewCount;
        this.windowEnd = windowEnd;
    }
}