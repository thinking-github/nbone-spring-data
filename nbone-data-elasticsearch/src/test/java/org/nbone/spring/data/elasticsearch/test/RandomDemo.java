package org.nbone.spring.data.elasticsearch.test;

import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.UUIDs;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

/**
 * Elasticsearch Random UUID
 * @author chenyicheng
 * @version 1.0
 * @since 2019/1/17
 */
public class RandomDemo {

    public static void main(String[] args) {


        System.out.println(772822416 % 1);
        System.out.println(772822417 % 1);
        System.out.println(772822418 % 1);

        System.out.println(0 % 1);

        System.out.println(Randomness.get().nextInt());
        System.out.println(Randomness.get().nextInt());
        System.out.println(Randomness.get().nextInt());


        System.out.println("UUID-");
        System.out.println(UUIDs.base64UUID());
        System.out.println(UUIDs.base64UUID().length());
        System.out.println(UUIDs.randomBase64UUID());
        System.out.println(UUIDs.randomBase64UUID().length());

        System.out.println(UUID.randomUUID().toString());
        System.out.println(UUID.randomUUID().toString().replace("-","").length());



    }

}
