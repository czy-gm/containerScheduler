package com.tianchi.django.calculate;

import com.google.common.collect.Lists;
import com.tianchi.django.common.enums.Resource;
import com.tianchi.django.common.pojo.*;
import com.tianchi.django.common.pojo.associate.PodPreAlloc;
import com.tianchi.django.common.utils.RuleUtils;
import com.tianchi.django.common.utils.ScheduleUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;


import java.util.*;

import static java.util.stream.Collectors.*;

@Slf4j
public class Utils {

    public static int gpuValue;

    public static int cpuValue;

    public static int ramValue;

    public static int diskValue;

    public static boolean fillOnePod(NodeAndPod nodeAndPod, Pod pod, int maxInstancePerNodeLimit) {

        if (!resourceFillOnePod(nodeAndPod, pod)) {
            return false;
        }

        if (!layoutFillOnePod(maxInstancePerNodeLimit, nodeAndPod, pod)) {
            return false;
        }

        PodPreAlloc podPreAlloc = cgroupFillOnePod(nodeAndPod, pod);
        if (!podPreAlloc.isSatisfy()) {
            return false;
        }

        if (CollectionUtils.isNotEmpty(podPreAlloc.getCpus())) {
            pod.setCpuIDs(podPreAlloc.getCpus());
            nodeAndPod.useCpu(podPreAlloc.getCpus());
        }

        nodeAndPod.minusResource(pod);
        nodeAndPod.addPodNum();
        nodeAndPod.addPodNumOfGroup(pod.getGroup());
        nodeAndPod.getPods().add(pod);

        return true;

    }


    public static boolean resourceFillOnePod(NodeAndPod nodeAndPod, Pod pod) {

        if (nodeAndPod.getSurplusGpu() < pod.getGpu()) {
            return false;
        }

        if (nodeAndPod.getSurplusCpu() < pod.getCpu()) {
            return false;
        }

        if (nodeAndPod.getSurplusRam() < pod.getRam()) {
            return false;
        }

        if (nodeAndPod.getSurplusDisk() < pod.getDisk()) {
            return false;
        }

        return nodeAndPod.getNode().getEni() - nodeAndPod.getPodNum() >= 1;
    }


    public static boolean layoutFillOnePod(int maxInstancePerNodeLimit, List<Pod> pods, Pod verifyPod) {

        return pods.stream().filter(pod -> pod.getGroup().equals(verifyPod.getGroup())).count() + 1 <= maxInstancePerNodeLimit;
    }

    public static boolean layoutFillOnePod(int maxInstancePerNodeLimit, NodeAndPod nodeAndPod, Pod verifyPod) {

        return nodeAndPod.getPodNumOfGroup(verifyPod.getGroup()) + 1 <= maxInstancePerNodeLimit;

    }


    public static PodPreAlloc cgroupFillOnePod(NodeAndPod nap, Pod pod) {

        Node node = nap.getNode();

        if (CollectionUtils.isEmpty(node.getTopologies())) {
            return PodPreAlloc.EMPTY_SATISFY;
        }

        List<Topology> useableTopologies = nap.getSurplusResource();
        //简单快速判断:当前宿主机剩余cpu已经不足以分配给该容器
        if (useableTopologies.size() < pod.getCpu()) {
            return PodPreAlloc.EMPTY_NOT_SATISFY;
        }

        //按照socket划分topologys，<socket,topologys>
        Map<Integer, List<Topology>> socketMap = useableTopologies.stream().collect(groupingBy(Topology::getSocket));

        for (Map.Entry<Integer, List<Topology>> socketEntry : socketMap.entrySet()) {

            List<Topology> socketTopologys = socketEntry.getValue();

            //当前socket下cpu不足以满足容器分配，规避跨socket绑定cpu问题
            if (socketTopologys.size() < pod.getCpu()) {
                continue;
            }

            //同一socket下按照core划分topologys，<core,topologys>
            Map<Integer, List<Topology>> coreMap = socketTopologys.stream().collect(groupingBy(Topology::getCore));

            //当前socket下不同core绑定cpu资源不足以满足容器分配，规避同一容器同core分配cpu问题。
            if (coreMap.size() < pod.getCpu()) {
                continue;
            }

            List<Pair<Integer, Integer>> coreTopoNum = new ArrayList<>();
            for (Map.Entry<Integer, List<Topology>> coreEntry : coreMap.entrySet()) {
                coreTopoNum.add(ImmutablePair.of(coreEntry.getKey(), coreEntry.getValue().size()));
            }

            Comparator<Pair<Integer, Integer>> comp =   Collections.reverseOrder(Comparator.comparingInt(Pair::getValue));
            coreTopoNum.sort(comp);

            List<Integer> cpus = Lists.newArrayList();

            for (Pair<Integer, Integer> coreAndNum : coreTopoNum) {

                cpus.add(coreMap.get(coreAndNum.getKey()).get(0).getCpu());

                if (cpus.size() == pod.getCpu()) {
                    break;
                }
            }
            //为此容器分配cpu
            return PodPreAlloc.builder().satisfy(true).cpus(cpus).build();

        }

        /*List<Integer> cpus = Lists.newArrayList();
        for (Map.Entry<Integer, List<Topology>> socketEntry : socketMap.entrySet()) {

            boolean isSatisfy = false;
            for (Topology topology : socketEntry.getValue()) {
                cpus.add(topology.getCpu());
                if (cpus.size() == pod.getCpu()) {
                    isSatisfy = true;
                    break;
                }
            }
            if (isSatisfy) {
                break;
            }
        }

        return PodPreAlloc.builder().satisfy(true).cpus(cpus).build();


         */

        return PodPreAlloc.EMPTY_NOT_SATISFY;
    }


    public static double calcMatchScoreUseCompleteResource(NodeAndPod nodeAndPod, Pod pod, Rule rule) {

        Map<Resource, Map<String, Integer>> scoreMap = RuleUtils.toResourceScoreMap(rule);


        if (!resourceFillOnePod(nodeAndPod, pod)) {
            return Integer.MAX_VALUE;
        }
        Node node = nodeAndPod.getNode();

        double gpuRatio = node.getGpu() == 0 ? Integer.MAX_VALUE : (double)node.getGpu() / pod.getGpu();
        double cpuRatio = (double)node.getCpu() / pod.getCpu();
        double ramRatio = (double)node.getRam() / pod.getRam();
        double diskRatio = (double)node.getDisk() / pod.getDisk();

        double minRatio = Math.min(gpuRatio, Math.min(cpuRatio, Math.min(ramRatio, diskRatio)));
        double score = 0;
        if (gpuRatio != minRatio) {
            score += ((double)node.getGpu() / minRatio - pod.getGpu()) * gpuValue ;
        }
        if (cpuRatio != minRatio) {
            score += ((double)node.getCpu() / minRatio - pod.getCpu()) * cpuValue;
        }
        if (ramRatio != minRatio) {
            score += ((double)node.getRam() / minRatio - pod.getRam()) * ramValue;
        }
        if (diskRatio != minRatio) {
            score += ((double)node.getDisk() / minRatio - pod.getDisk()) * diskValue;
        }

        return score;
    }

    public static double calcMatchScore(NodeAndPod nodeAndPod, Pod pod, Rule rule) {

        Map<Resource, Map<String, Integer>> scoreMap = RuleUtils.toResourceScoreMap(rule);

        if (!resourceFillOnePod(nodeAndPod, pod)) {
            return Integer.MAX_VALUE;
        }
        double gpuRatio = nodeAndPod.getSurplusGpu() == 0 ? Integer.MAX_VALUE : (double)nodeAndPod.getSurplusGpu() / pod.getGpu();
        double cpuRatio = (double)nodeAndPod.getSurplusCpu() / pod.getCpu();
        double ramRatio = (double)nodeAndPod.getSurplusRam() / pod.getRam();
        //double diskRatio = (double)nodeAndPod.getSurplusDisk() / pod.getDisk();

        double minRatio = Math.min(gpuRatio, Math.min(cpuRatio, ramRatio));
        double score = 0;
        if (gpuRatio != minRatio) {
            score += ((double)nodeAndPod.getSurplusGpu() / minRatio - pod.getGpu()) * gpuValue;
        }
        if (cpuRatio != minRatio) {
            score += ((double)nodeAndPod.getSurplusCpu() / minRatio - pod.getCpu()) * cpuValue;
        }
        if (ramRatio != minRatio) {
            score += ((double)nodeAndPod.getSurplusRam() / minRatio - pod.getRam()) * ramValue;
        }

        return score;
    }

    public static double calcMatchScore(NodeWithPod nodeWithPod, Pod pod, Rule rule) {

        Map<Resource, Map<String, Integer>> scoreMap = RuleUtils.toResourceScoreMap(rule);

        if (!ScheduleUtils.resourceFillOnePod(nodeWithPod, pod)) {
            return Integer.MAX_VALUE;
        }

        int surplusGpu = nodeWithPod.getNode().getGpu() - nodeWithPod.getPods().stream().mapToInt(Pod::getGpu).sum();
        int surplusCpu = nodeWithPod.getNode().getCpu() - nodeWithPod.getPods().stream().mapToInt(Pod::getCpu).sum();
        int surplusRam = nodeWithPod.getNode().getRam() - nodeWithPod.getPods().stream().mapToInt(Pod::getRam).sum();
        int surplusDisk = nodeWithPod.getNode().getDisk() - nodeWithPod.getPods().stream().mapToInt(Pod::getDisk).sum();

        double gpuRatio = surplusGpu == 0 ? Integer.MAX_VALUE : (double)surplusGpu / pod.getGpu();
        double cpuRatio = (double)surplusCpu / pod.getCpu();
        double ramRatio = (double)surplusRam / pod.getRam();
        double diskRatio = (double)surplusDisk / pod.getDisk();

        double minRatio = Math.min(gpuRatio, Math.min(cpuRatio, Math.min(ramRatio, diskRatio)));
        double score = 0;
        if (gpuRatio != minRatio) {
            score += ((double)surplusGpu / minRatio - pod.getGpu()) * gpuValue;
        }
        if (cpuRatio != minRatio) {
            score += ((double)surplusCpu / minRatio - pod.getCpu()) * cpuValue;
        }
        if (ramRatio != minRatio) {
            score += ((double)surplusRam / minRatio - pod.getRam()) * ramValue;
        }
        if (diskRatio != minRatio) {
            score += ((double)surplusDisk / minRatio - pod.getDisk()) * diskValue;
        }
        return score;
    }

    public static void sortNodes(List<NodeAndPod> nodeAndPods, Pod pod, Rule rule) {

        Comparator<NodeAndPod> first = Collections.reverseOrder(Comparator.comparingInt(nap -> CollectionUtils.isNotEmpty(nap.getPods()) ? 1 : 0));
        Comparator<NodeAndPod> second = Comparator.comparingDouble(nap -> calcMatchScoreUseCompleteResource(nap, pod, rule));

        nodeAndPods.sort(first.thenComparing(second));
    }

    public static void sortNode(List<NodeWithPod> nodeWithPods, Pod pod, Rule rule) {

        Comparator<NodeWithPod> first = Collections.reverseOrder(Comparator.comparingInt(nap -> CollectionUtils.isNotEmpty(nap.getPods()) ? 1 : 0));
        Comparator<NodeWithPod> second = Comparator.comparingDouble(nap -> calcMatchScore(nap, pod, rule));

        nodeWithPods.sort(first.thenComparing(second));

    }

    public static void sortNode(List<NodeAndPod> nodeAndPods, Pod pod, NodeAndPod lastSelectNode, Pod lastAllocationPod, Rule rule) {

        List<NodeAndPod> emptyNaps = new ArrayList<>();
        List<NodeAndPod> noEmptyNaps = new ArrayList<>();
        for (NodeAndPod nodeAndPod : nodeAndPods) {
            if (CollectionUtils.isEmpty(nodeAndPod.getPods())) {
                emptyNaps.add(nodeAndPod);
            }
            else {
                noEmptyNaps.add(nodeAndPod);
            }
        }

        Comparator<NodeAndPod> first = Collections.reverseOrder(Comparator.comparingInt(nap -> CollectionUtils.isNotEmpty(nap.getPods()) ? 1 : 0));
        Comparator<NodeAndPod> second = Comparator.comparingDouble(nap -> calcMatchScore(nap, pod, rule));
        Comparator<NodeAndPod> third = Comparator.comparingInt(nap -> nap.getNode().getCpu() * cpuValue + nap.getNode().getRam());


        if (lastAllocationPod != null && isEqualOfRequredResource(pod, lastAllocationPod)) {

            NodeAndPod cpLastSelectNode = lastSelectNode.copy();
            nodeAndPods.remove(lastSelectNode);

            int index = Collections.binarySearch(nodeAndPods, cpLastSelectNode, second);
            if (index < 0) index = -(index + 1);
            nodeAndPods.add(index, cpLastSelectNode);
        }
        else {
            noEmptyNaps.sort(second);
            emptyNaps.sort(second.thenComparing(third));
            nodeAndPods.clear();
            nodeAndPods.addAll(noEmptyNaps);
            nodeAndPods.addAll(emptyNaps);
            //nodeAndPods.sort(first.thenComparing(second).thenComparing(third));
        }
    }


    public static List<NodeAndPod> buildNodeAndPod(List<NodeWithPod> nwps) {

        List<NodeAndPod> naps = nwps.stream()
                .map(nwp -> {
                        List<Integer> cpuIds = nwp.getPods().stream().map(Pod::getCpuIDs).flatMap(Collection::stream).collect(toList());
                        return NodeAndPod.builder()
                                .surplusGpu(nwp.getNode().getGpu() - nwp.getPods().stream().mapToInt(Pod::getGpu).sum())
                                .surplusCpu(nwp.getNode().getCpu() - nwp.getPods().stream().mapToInt(Pod::getCpu).sum())
                                .surplusRam(nwp.getNode().getRam() - nwp.getPods().stream().mapToInt(Pod::getRam).sum())
                                .surplusDisk(nwp.getNode().getDisk() - nwp.getPods().stream().mapToInt(Pod::getDisk).sum())
                                .surplusResource(nwp.getNode().getTopologies().stream()
                                        .filter(topo -> !cpuIds.contains(topo.getCpu())).collect(toList()))
                                .node(nwp.getNode()).pods(nwp.getPods()).podNum(nwp.getPods().size())
                                .podNumOfGroupMap(new HashMap<>()).build();
                        }
                        ).collect(toList());

        for (NodeAndPod nap : naps) {
            for (Pod pod : nap.getPods()) {

                nap.addPodNumOfGroup(pod.getGroup());
            }
        }

        return naps;
    }

    public static Pair<Double, Double> calcResourceUsedRatio(NodeAndPod nodeAndPod) {

        if (nodeAndPod.getPodNum() == 0) {
            return ImmutablePair.of(0.0, 0.0);
        }
        Node node = nodeAndPod.getNode();

        double cpuUsedRatio = 100f * (node.getCpu() - nodeAndPod.getSurplusCpu()) / node.getCpu();
        double ramUsedRatio = 100f * (node.getRam() - nodeAndPod.getSurplusRam()) / node.getRam();

        return ImmutablePair.of(cpuUsedRatio, ramUsedRatio);

    }

    public static void calcResourceUsedRatios(List<NodeAndPod> nodeAndPods) {

        float totalGpuUsedRatio = 0;
        float totalCpuUsedRatio = 0;
        float totalRamUsedRatio = 0;
        float totalDiskUsedRatio = 0;

        int nodeNum = 0;
        for (NodeAndPod nodeAndPod : nodeAndPods) {
            if (nodeAndPod.getPodNum() == 0) {
                continue;
            }
            Node node = nodeAndPod.getNode();

            String sn = node.getSn();
            float gpuUsedRatio = 100f * (node.getGpu() - nodeAndPod.getSurplusGpu()) / node.getGpu();
            float cpuUsedRatio = 100f * (node.getCpu() - nodeAndPod.getSurplusCpu()) / node.getCpu();
            float ramUsedRatio = 100f * (node.getRam() - nodeAndPod.getSurplusRam()) / node.getRam();
            float diskUsedRatio = 100f * (node.getDisk() - nodeAndPod.getSurplusDisk()) / node.getDisk();
            log.info("node sn:{}, UsedRatio:[gpu: {}%, cpu: {}%, ram: {}%, disk: {}%], nodeRate:{}", sn, gpuUsedRatio, cpuUsedRatio, ramUsedRatio, diskUsedRatio, (double)node.getRam() / node.getCpu());

            totalGpuUsedRatio += gpuUsedRatio;
            totalCpuUsedRatio += cpuUsedRatio;
            totalRamUsedRatio += ramUsedRatio;
            totalDiskUsedRatio += diskUsedRatio;
            nodeNum++;
        }

        log.info("total usedratio:[gpu: {}%, cpu: {}%, ram: {}%, disk: {}%]", totalGpuUsedRatio / nodeNum, totalCpuUsedRatio / nodeNum, totalRamUsedRatio / nodeNum, totalDiskUsedRatio / nodeNum);
    }

    public static void nodeRourceRatio(List<NodeAndPod> nodeAndPods) {

        nodeAndPods.stream().map(nap -> (double)nap.getNode().getRam() / nap.getNode().getCpu()).sorted().distinct().forEach($ -> log.info("ratio :{}", $));

    }

    private static boolean isEqualOfRequredResource(Pod firstPod, Pod secondPod) {

        if (firstPod.getGpu() != secondPod.getGpu()) {
            return false;
        }

        if (firstPod.getCpu() != secondPod.getCpu()) {
            return false;
        }

        if (firstPod.getRam() != secondPod.getRam()) {
            return false;
        }

        return firstPod.getDisk() == secondPod.getDisk();
    }

    public static PodPreAlloc cgroupFillOnePod(NodeWithPod nwp, Pod pod) {

        Node node = nwp.getNode();

        if (CollectionUtils.isEmpty(node.getTopologies())) {
            return PodPreAlloc.EMPTY_SATISFY;
        }

        List<Pod> pods = nwp.getPods();

        //当前宿主机已经使用的cpu
        Set<Integer> usedCpuSet = pods.stream().map(Pod::getCpuIDs).flatMap(Collection::stream).collect(toSet());

        //剩余可用的cpu数量
        List<Topology> useableTopologies = node.getTopologies().stream().filter(topology -> !usedCpuSet.contains(topology.getCpu())).collect(toList());

        //简单快速判断:当前宿主机剩余cpu已经不足以分配给该容器
        if (useableTopologies.size() < pod.getCpu()) {
            return PodPreAlloc.EMPTY_NOT_SATISFY;
        }

        //按照socket划分topologys，<socket,topologys>
        Map<Integer, List<Topology>> socketMap = useableTopologies.stream().collect(groupingBy(Topology::getSocket));

        for (Map.Entry<Integer, List<Topology>> socketEntry : socketMap.entrySet()) {

            List<Topology> socketTopologys = socketEntry.getValue();

            //当前socket下cpu不足以满足容器分配，规避跨socket绑定cpu问题
            if (socketTopologys.size() < pod.getCpu()) {
                continue;
            }

            //同一socket下按照core划分topologys，<core,topologys>
            Map<Integer, List<Topology>> coreMap = socketTopologys.stream().collect(groupingBy(Topology::getCore));

            //当前socket下不同core绑定cpu资源不足以满足容器分配，规避同一容器同core分配cpu问题。
            if (coreMap.size() < pod.getCpu()) {
                continue;
            }

            List<Pair<Integer, Integer>> coreTopoNum = new ArrayList<>();
            for (Map.Entry<Integer, List<Topology>> coreEntry : coreMap.entrySet()) {
                coreTopoNum.add(ImmutablePair.of(coreEntry.getKey(), coreEntry.getValue().size()));
            }

            Comparator<Pair<Integer, Integer>> comp =   Collections.reverseOrder(Comparator.comparingInt(Pair::getValue));
            coreTopoNum.sort(comp);

            List<Integer> cpus = Lists.newArrayList();

            for (Pair<Integer, Integer> coreAndNum : coreTopoNum) {

                cpus.add(coreMap.get(coreAndNum.getKey()).get(0).getCpu());

                if (cpus.size() == pod.getCpu()) {
                    break;
                }
            }

            //为此容器分配cpu
            return PodPreAlloc.builder().satisfy(true).cpus(cpus).build();

        }
        List<Integer> cpus = Lists.newArrayList();

        for (Map.Entry<Integer, List<Topology>> socketEntry : socketMap.entrySet()) {

            boolean isSatisfy = false;
            for (Topology topology : socketEntry.getValue()) {
                cpus.add(topology.getCpu());
                if (cpus.size() == pod.getCpu()) {
                    isSatisfy = true;
                    break;
                }
            }
            if (isSatisfy) {
                break;
            }
        }

        return PodPreAlloc.builder().satisfy(true).cpus(cpus).build();

        //return PodPreAlloc.EMPTY_NOT_SATISFY;
    }

    public static boolean isValidOfCgroup(NodeWithPod nodeWithPod, Pod pod) {

        if (CollectionUtils.isEmpty(nodeWithPod.getNode().getTopologies())) {
            return true;
        }

        List<Topology> podTopos = nodeWithPod.getNode().getTopologies().stream().filter(topo -> pod.getCpuIDs().contains(topo.getCpu())).collect(toList());

        if (podTopos.size() < pod.getCpu()) {
            return false;
        }

        for (Topology topology : podTopos) {

            if (podTopos.stream().filter(podTopo -> podTopo.getCore() == topology.getCore()).count() >= 2) {
                return false;
            }
        }
        return true;
    }

    public static void sortPod(List<Pod> pods, NodeAndPod nodeAndPod, Rule rule) {

        Comparator<Pod> first = Comparator.comparingDouble(pod -> calcMatchScore(nodeAndPod, pod, rule));

        pods.sort(first);

    }

}
