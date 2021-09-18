package com.tianchi.django.calculate;

import com.tianchi.django.common.pojo.Pod;
import lombok.*;

@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class AlloPod {

    private NodeAndPod alloNap;

    private Pod pod;

    private double matchScore;
}
