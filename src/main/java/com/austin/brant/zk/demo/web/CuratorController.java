package com.austin.brant.zk.demo.web;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.austin.brant.zk.demo.utils.DistributedLockByZk;

import lombok.extern.slf4j.Slf4j;

/**
 * 测试分布式锁的controller
 *
 * @author austin-brant
 * @since 2019/7/12 19:16
 */
@RestController
@RequestMapping(value = "/")
@Slf4j
public class CuratorController {

    @Autowired
    private DistributedLockByZk distributedLockByZk;

    @GetMapping("/lock")
    public Boolean getLock(@RequestParam String path) {
        distributedLockByZk.acquireDistributedLock(path);
        return Boolean.TRUE;
    }

    @GetMapping(value = "/release")
    private boolean releaseLock(@RequestParam String path) {
        distributedLockByZk.releaseDistributedLock(path);
        return true;
    }

}
