package com.tianchi.django.calculate;

import com.tianchi.django.common.pojo.Node;
import com.tianchi.django.common.pojo.NodeWithPod;
import com.tianchi.django.common.pojo.Pod;
import com.tianchi.django.common.pojo.Topology;
import lombok.*;
import org.apache.commons.collections4.ListUtils;
import org.apache.commons.collections4.MapUtils;

import java.util.*;

import static java.util.stream.Collectors.toList;

@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class NodeAndPod{

    private Node node;//node信息

    private List<Pod> pods;//该宿主机上的pod信息

    private Map<String, Integer> podNumOfGroupMap = new HashMap<>();

    private int podNum;

    private int surplusGpu;

    private int surplusCpu;

    private int surplusRam;

    private int surplusDisk;

    private List<Topology> surplusResource = new LinkedList<>();

    public void minusResource(Pod pod) {
        surplusGpu -= pod.getGpu();
        surplusCpu -= pod.getCpu();
        surplusRam -= pod.getRam();
        surplusDisk -= pod.getDisk();
    }

    public void plusResource(Pod pod) {
        surplusGpu += pod.getGpu();
        surplusCpu += pod.getCpu();
        surplusRam += pod.getRam();
        surplusDisk += pod.getDisk();
    }

    public void addPodNum() {
        podNum++;
    }

    public int getPodNumOfGroup(String group) {
        return podNumOfGroupMap.getOrDefault(group, 0);
    }

    public void addPodNumOfGroup(String group) {
        int podNum = podNumOfGroupMap.getOrDefault(group, 0);
        podNumOfGroupMap.put(group, podNum + 1);
    }

    public void minusPodNumOfGroup(String group) {
        int num = podNumOfGroupMap.getOrDefault(group, 0);
        if (num > 0) {
            podNumOfGroupMap.put(group, num - 1);
        }
    }

    public void useCpu(List<Integer> usedCpu) {
        surplusResource = surplusResource.stream().filter(topology -> !usedCpu.contains(topology.getCpu())).collect(toList());
    }

    public List<Integer> useCpu(int num) {
        List<Integer> usedCpus;
        List<Topology> usedCpu = surplusResource.subList(0, num);
        usedCpus = usedCpu.stream().map(Topology::getCpu).collect(toList());
        surplusResource.removeAll(usedCpu);
        return usedCpus;
    }

    public void freeCpu(List<Integer> freeCpus) {
        List<Topology> freeTopo = node.getTopologies().stream().filter(topology -> freeCpus.contains(topology.getCpu())).collect(toList());
        surplusResource.addAll(freeTopo);
    }

    public NodeAndPod copy() {
        return NodeAndPod.builder().node(node.copy()).pods(ListUtils.emptyIfNull(pods).stream().map(Pod::copy).collect(toList())).
                podNum(podNum).surplusGpu(surplusGpu).surplusCpu(surplusCpu).surplusRam(surplusRam).surplusDisk(surplusDisk).
                podNumOfGroupMap(new HashMap<>(MapUtils.emptyIfNull(podNumOfGroupMap))).surplusResource(ListUtils.emptyIfNull(surplusResource).
                stream().map(Topology::copy).collect(toList())).build();
    }

    public NodeWithPod incompleteCopy() {
        return NodeWithPod.builder().node(node.copy()).pods(ListUtils.emptyIfNull(pods).stream().map(Pod::copy).collect(toList())).build();
    }
}
