package Bean;

import java.io.Serializable;

public class UserBehavior implements Serializable{
    public long userId;
    public long itemId;
    public long categoryId;
    public String behavior;
    public long timeStamp;

    public UserBehavior(){

    }

    public long getUserId() {
        return userId;
    }

    public void setUserId(long userId) {
        this.userId = userId;
    }

    public long getItemId() {
        return itemId;
    }

    public void setItemId(long itemId) {
        this.itemId = itemId;
    }

    public long getCategoryId() {
        return categoryId;
    }

    public void setCategoryId(long categoryId) {
        this.categoryId = categoryId;
    }

    public String getBehavior() {
        return behavior;
    }

    public void setBehavior(String behavior) {
        this.behavior = behavior;
    }

    public long getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(long timeStamp) {
        this.timeStamp = timeStamp;
    }

    public String toString(){
        return "UserBehavior [userId="+userId+" categoryId="+categoryId+" itemid="+itemId+" behavior="+behavior+"]";
    }

}
