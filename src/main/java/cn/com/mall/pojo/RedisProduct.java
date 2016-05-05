package cn.com.mall.pojo;

/**
 * Created by aaron on 16-5-6.
 */
public class RedisProduct {

    // product category_productId
    private String key;

    // product name
    private String value;

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return "RedisProduct [key=" + key + ", value=" + value + "]";
    }
}
