package com.tianchi.django.calculate;


import com.google.common.collect.Lists;
import com.tianchi.django.common.pojo.Pod;
import com.tianchi.django.common.pojo.Rule;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.util.*;
import java.util.stream.Collectors;


@Slf4j
public class AllocationStrategy {

    public static final double INIT_TEMPERATURE = 100;

    public static final double TERMINAL_TEMPERATURE = 1;

    public static final int EXCHANGE_NUM = 10; //10

    public static final double DELTA = 0.88; //0.88

    public static final int LOOP = 4; //2

    public static final ThreadLocal<Map<String, Integer>> ALL_MAX_INSTANCE_PER_NODE_LIMIT = new ThreadLocal<>();


    public static void newAllocationForReschedule(List<NodeAndPod> nodeAndPods, List<Pod> pods, Rule rule) {

        pods.forEach(pod -> pod.setCpuIDs(Collections.emptyList()));

        int first, second;
        Random random = new Random();

        for (int i = 0; i < EXCHANGE_NUM; i++) {

            do {

                first = random.nextInt(pods.size());
                second = random.nextInt(pods.size());

            }while(first == second);

            Collections.swap(pods, first, second);
        }

        List<Pod> newPods = pods.stream().map(Pod::copy).collect(Collectors.toList());
        allocation(nodeAndPods, newPods, rule);

    }

    public static void newAllocationForSchedule(List<NodeAndPod> nodeAndPods, List<Pod> pods, Rule rule) {

        pods.forEach(pod -> pod.setCpuIDs(Collections.emptyList()));

        int first, second;
        Random random = new Random();

        for (int i = 0; i < EXCHANGE_NUM; i++) {

            do {

                first = random.nextInt(pods.size());
                second = random.nextInt(pods.size());

            }while(first == second);

            Collections.swap(pods, first, second);
        }

        allocation(nodeAndPods, pods, rule);

    }

    public static void allocation(List<NodeAndPod> nodeAndPods, List<Pod> pods, Rule rule) {

        Map<String, Integer> allMaxInstancePerNodeLimit = ALL_MAX_INSTANCE_PER_NODE_LIMIT.get();

        List<Pod> podList = pods.stream().map(Pod::copy).collect(Collectors.toList());

        for (NodeAndPod nodeAndPod : nodeAndPods) {

            if (CollectionUtils.isEmpty(podList)) {
                break;
            }
            Utils.sortPod(podList, nodeAndPod, rule);

            List<Pod> selectedPods = new ArrayList<>();
            for (Pod pod: podList) {

                int maxInstancePerNodeLimit = allMaxInstancePerNodeLimit.get(pod.getGroup());
                //long beg = System.currentTimeMillis();
                if (Utils.fillOnePod(nodeAndPod, pod, maxInstancePerNodeLimit)) {
                    selectedPods.add(pod);
                }
                //System.out.println("fillOnePod time:" + (System.currentTimeMillis() - beg));
            }

            podList.removeAll(selectedPods);
        }


        /*NodeAndPod lastSelectNode = null;
        Pod lastAllocationPod = null;
        for (Pod pod : pods) {

            String group = pod.getGroup();

            int maxInstancePerNodeLimit = allMaxInstancePerNodeLimit.get(group);

            Utils.sortNode(nodeAndPods, pod, lastSelectNode, lastAllocationPod, rule);


            for (NodeAndPod nap : nodeAndPods) {

                if (Utils.fillOnePod(nap, pod, maxInstancePerNodeLimit)) {
                    lastAllocationPod = pod;
                    lastSelectNode = nap;
                    break;
                }
            }

        }
        List<NodeAndPod> lowRateNaps = new ArrayList<>();
        List<Pod> remainPods = new ArrayList<>();
        for (NodeAndPod nap : nodeAndPods) {

            Pair<Double, Double> ratio = Utils.calcResourceUsedRatio(nap);
            if (ratio.getKey() < 85) {
                remainPods.addAll(nap.getPods());
                nap.setSurplusGpu(nap.getNode().getGpu());
                nap.setSurplusCpu(nap.getNode().getCpu());
                nap.setSurplusRam(nap.getNode().getRam());
                nap.setSurplusDisk(nap.getNode().getDisk());
                nap.setSurplusResource(nap.getNode().getTopologies());
                nap.setPodNumOfGroupMap(new HashMap<>());
                nap.setPodNum(0);
                nap.setPods(Lists.newArrayList());
                lowRateNaps.add(nap);
            }
        }

        for (Pod pod: remainPods) {
            pod.setCpuIDs(Lists.newArrayList());
        }


        for (Pod pod : remainPods) {

            String group = pod.getGroup();

            int maxInstancePerNodeLimit = allMaxInstancePerNodeLimit.get(group);
            Utils.sortNode(nodeAndPods, pod, null, null, rule);

            for (NodeAndPod nap : lowRateNaps) {

                if (Utils.fillOnePod(nap, pod, maxInstancePerNodeLimit)) {
                    break;
                }

            }

        }

         */

    }


    /*private static void dp(List<NodeAndPod> nodeAndPodList, List<Pod> pods, Rule rule) {

        Map<String, Integer> allMaxInstancePerNodeLimit = AllocationStrategy.ALL_MAX_INSTANCE_PER_NODE_LIMIT.get();

        int[][] allo = new int[1000][1000];
        String[][] alloIds = new String[1000][1000];

        log.info("pods is {}", pods.size());

        nodeAndPodList.sort(Collections.reverseOrder(Comparator.comparingInt(NodeAndPod::getSurplusCpu)));

        while(CollectionUtils.isNotEmpty(pods)) {

            int maxS = 0;
            NodeAndPod selectNap = null;
            String selectAlloIds = "";

            log.info("pods size: {}", pods.size());
            for (NodeAndPod nodeAndPod : nodeAndPodList) {

                if (CollectionUtils.isEmpty(pods)) {
                    break;
                }
                if (CollectionUtils.isNotEmpty(nodeAndPod.getPods())) {
                    continue;
                }


                int nodeCpu = nodeAndPod.getNode().getCpu();
                int nodeRam = nodeAndPod.getNode().getRam();

                for(int[] tmp : allo) {
                    Arrays.fill(tmp, 0);
                }
                for (String[] tmp : alloIds) {
                    Arrays.fill(tmp, "");
                }
                Map<String, List<Pod>> podsByGroup = new HashMap<>();

                for (Pod pod : pods) {
                    List<Pod> groupPods = podsByGroup.getOrDefault(pod.getGroup(), new ArrayList<>());
                    groupPods.add(pod);
                    podsByGroup.put(pod.getGroup(),groupPods);
                }

                for (Map.Entry<String, List<Pod>> entry : podsByGroup.entrySet()) {

                    double maxScore = 0.0;

                    for (Pod pod : entry.getValue()) {
                        int maxInstancePerNodeLimit = allMaxInstancePerNodeLimit.get(pod.getGroup());
                        int podCpu = pod.getCpu();
                        int podRam = pod.getRam();

                        //log.info("index:{}", t);
                        for (int i = nodeCpu; i >= podCpu; i--) {

                            for (int j = nodeRam; j >= podRam; j--) {

                                if (StringUtils.isNotBlank(alloIds[i - podCpu][j - podRam])) {
                                    List<Pod> selectPods = getPodsByGroup(pods, alloIds[i - podCpu][j - podRam]);
                                    if (!Utils.layoutFillOnePod(maxInstancePerNodeLimit, selectPods, pod)) {
                                        continue;
                                    }


                                    NodeAndPod tmpNap = nodeAndPod.copy();
                                    selectPods.forEach(selectPod -> tmpNap.useCpu(selectPod.getCpuIDs()));
                                    PodPreAlloc podPreAlloc = Utils.cgroupFillOnePod(tmpNap, pod);
                                    if (!podPreAlloc.isSatisfy()) {
                                        continue;
                                    }



                                }

                                if (allo[i][j] < allo[i - podCpu][j - podRam] + 2 * podCpu + podRam) {
                                    alloIds[i][j] = alloIds[i - podCpu][j - podRam] == null ? "" : alloIds[i - podCpu][j - podRam].concat(pod.getGroup().concat(","));
                                }
                                allo[i][j] = Math.max(allo[i][j], allo[i - podCpu][j - podRam] + 2 * podCpu + podRam);
                            }
                        }

                        if (maxScore == allo[nodeCpu][nodeRam]) {
                            break;
                        }
                        maxScore = allo[nodeCpu][nodeRam];
                    }

                }
                if (allo[nodeCpu][nodeRam] > maxS) {
                    //log.info("in");
                    selectNap = nodeAndPod;
                    selectAlloIds = alloIds[nodeCpu][nodeRam];
                    maxS = allo[nodeCpu][nodeRam];
                }

            }
            List<Pod> selectPods = getPodsByGroup(pods, selectAlloIds);
            for (int i = 0; i < selectPods.size(); i++) {
                Pod selectPod = selectPods.get(i);
                int maxInstancePerNodeLimit = allMaxInstancePerNodeLimit.get(selectPod.getGroup());
                if (!Utils.fillOnePod(selectNap, selectPod, maxInstancePerNodeLimit)) {
                    log.error("allo pod failed. nodeSn:{}, podSn:{}", selectNap.getNode().getSn(), selectPod.getPodSn());
                    selectNap.setPods(Lists.newArrayList());
                    pods.addAll(selectPods.subList(0, i));
                    break;
                }
                pods.remove(selectPod);
            }
        }
    }

    private static List<Pod> getPodsByGroup(List<Pod> pods, String str) {

        List<Pod> selectPods = new ArrayList<>();
        String[] groups = str.split(",");

        for (String group : groups) {
            if (StringUtils.isNotBlank(group)) {
                for (Pod pod : pods) {
                    if (pod.getGroup().equals(group)) {
                        selectPods.add(pod);
                        break;
                    }
                }
                pods.remove(selectPods.get(selectPods.size() - 1));
            }
        }
        pods.addAll(selectPods);
        return selectPods;
    }
*/

}
