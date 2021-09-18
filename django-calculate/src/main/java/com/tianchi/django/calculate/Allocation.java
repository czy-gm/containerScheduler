package com.tianchi.django.calculate;

import com.tianchi.django.common.pojo.Pod;
import com.tianchi.django.common.pojo.RescheduleResult;
import lombok.*;

import java.util.ArrayList;
import java.util.List;
import static java.util.stream.Collectors.toList;

@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Allocation {

    private List<NodeAndPod> nodeAndPods;

    private List<Pod> pods;

    private int score;

    private List<RescheduleResult> rescheduleResults;

    public void setAndCopy(List<NodeAndPod> nodeAndPods, List<Pod> podList, int score, List<RescheduleResult> rescheduleResults) {

        setAndCopy(nodeAndPods, podList, score);

        this.rescheduleResults = new ArrayList<>(rescheduleResults);

    }

    public void setAndCopy(List<NodeAndPod> nodeAndPodList, List<Pod> podList, int score) {

        nodeAndPods = nodeAndPodList.parallelStream().map(NodeAndPod::copy).collect(toList());

        pods = podList.parallelStream().map(Pod::copy).collect(toList());

        this.score = score;

    }
}
