package org.example;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import redis.clients.jedis.*;
import redis.clients.jedis.params.ScanParams;
import redis.clients.jedis.resps.ScanResult;

import java.util.*;

public class RedisConverter {

    public static void main(String[] args) {
        // 1. Redis Cluster 연결
        Set<HostAndPort> clusterNodes = Set.of(
                new HostAndPort("192.168.150.115", 7002),
                new HostAndPort("192.168.150.120", 7002),
                new HostAndPort("192.168.150.125", 7002),
                new HostAndPort("192.168.150.115", 7003),
                new HostAndPort("192.168.150.120", 7003),
                new HostAndPort("192.168.150.125", 7003)
        );

        JedisCluster jedisCluster = new JedisCluster(clusterNodes);
        ObjectMapper mapper = new ObjectMapper();

        // 2. 각 노드에 직접 연결해서 scan 처리
        for (HostAndPort node : clusterNodes) {
            try (Jedis jedis = new Jedis(node.getHost(), node.getPort())) {

                // 마스터 노드만 처리
                if (!jedis.info("replication").contains("role:master")) {
                    continue;
                }

                String cursor = ScanParams.SCAN_POINTER_START;
                ScanParams scanParams = new ScanParams().match("*").count(100);
                int convertedCount = 0;

                do {
                    ScanResult<String> scanResult = jedis.scan(cursor, scanParams);
                    List<String> keys = scanResult.getResult();

                    for (String key : keys) {
                        try {
                            String type = jedisCluster.type(key);
                            if ("string".equals(type)) {
                                String value = jedisCluster.get(key);
                                if (value != null && isJsonObject(value, mapper)) {
                                    Map<String, String> map = mapper.readValue(value, new TypeReference<Map<String, String>>() {});

                                    jedisCluster.set(key + ":backup", value);   // 기존 string백업
                                    jedisCluster.del(key);                      // 기존 string 제거
                                    jedisCluster.hset(key, map);                // hash 저장

                                    convertedCount++;
//                                    System.out.println("[" + convertedCount + "] Converted to Hash: " + key + " / " + map);
                                } else {
                                    System.out.println("Value is null or is not JsonObject(key: " + key + ")");
                                }
                            } else {
                                System.out.println("Value is not String(key: " + key + ")");
                            }
                        } catch (Exception e) {
                            System.err.println("Failed to convert key: " + key + " → " + e.getMessage());
                        }
                    }
                    cursor = scanResult.getCursor();
                } while (!cursor.equals(ScanParams.SCAN_POINTER_START));

            } catch (Exception e) {
                System.err.println("노드 연결 실패: " + node + " → " + e.getMessage());
            }
        }
        System.out.println("모든 JSON 문자열을 Redis Hash로 변환 완료");
    }

    // JSON 형식인지 확인
    private static boolean isJsonObject(String value, ObjectMapper mapper) {
        try {
            return mapper.readTree(value).isObject();
        } catch (JsonProcessingException e) {
            return false;
        }
    }
}