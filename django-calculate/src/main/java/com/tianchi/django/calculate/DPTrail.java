package com.tianchi.django.calculate;

import com.tianchi.django.common.pojo.Pod;
import lombok.*;

import java.util.ArrayList;
import java.util.List;

@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DPTrail {

    List<Pod> pods = new ArrayList<>();

    public void addPod(Pod pod) {
        pods.add(pod);
    }
}
