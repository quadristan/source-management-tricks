/**
 * Disclaymer:
 * I did this in a hurry just to check what could be done with gitlab data.
 * Considers this as a POC or as a research paper.
 * Dont use this in prod.
 */

import fetch from "cross-fetch";
import * as fs from "fs/promises";
import * as fsSync from "fs";
var random_name = require("node-random-name");

const GITLAB_PROJECT_PATH = process.env.GITLAB_PROJECT_PATH;
const GITLAB_PROJECT_ID = process.env.GITLAB_PROJECT_ID;
const GITALB_INSTANCE_URL = process.env.GITLAB_INSTANCE_URL;

const GITLAB_API_URL = `${GITALB_INSTANCE_URL}/v4`;
const GITLAB_GQL_URL = `${GITALB_INSTANCE_URL}/graphql`;
const TIME_PERIOD_DAYS = 2 * 365;

const ANONYMISE = false;

// const ignoreRules = new Set(["**/package.json"]);
const ignoreRules = new Set();

const anonymisedNames = new Map<string, string>();
function getAnonymisedName(name: string) {
  if (!ANONYMISE) {
    return name;
  }
  if (!anonymisedNames.has(name)) {
    anonymisedNames.set(name, random_name({ random: Math.random }));
  }
  return anonymisedNames.get(name)!;
}

function addDays(date: Date, days: number) {
  const dateCopy = new Date(date);
  dateCopy.setDate(date.getDate() + days);
  return dateCopy;
}

async function doOrCache<R>(file: string, action: () => Promise<R>) {
  const fullPath = `./cache/${file}.json`;
  if (fsSync.existsSync(fullPath)) {
    return JSON.parse(await fs.readFile(fullPath, "utf8")) as R;
  }
  const result = await action();
  await fs.writeFile(fullPath, JSON.stringify(result));
  return result;
}

async function getGitlabData<X>(queryString: string): Promise<X> {
  const query = await fetch(GITLAB_API_URL + queryString, {
    headers: {
      accept: "*/*",
      "content-type": "application/json",
      "PRIVATE-TOKEN": process.env.GITLAB_TOKEN,
    },
    method: "GET",
  });
  if (query.status >= 400) {
    console.log(query.status, query.statusText);
    console.log(await query.text());
    throw new Error("Gitlab query failed " + queryString);
  }
  return await query.json();
}

async function getGitlabPaginatedData<X>(queryString: string): Promise<X[]> {
  let page = 0;
  const baseQuery =
    GITLAB_API_URL +
    (queryString.includes("?")
      ? queryString + "&per_page=100"
      : queryString + "?per_page=100");

  const result = new Array<X>();

  while (true) {
    const query = await fetch(`${baseQuery}&page=${page}`, {
      headers: {
        accept: "*/*",
        "content-type": "application/json",
        "PRIVATE-TOKEN": process.env.GITLAB_TOKEN,
      },
      method: "GET",
    });
    page++;
    if (query.status >= 400) {
      console.log(query.status, query.statusText);
      console.log(await query.text());
      throw new Error("Gitlab query failed");
    }
    const r = await query.json();
    if (!Array.isArray(r)) {
      console.log(r);
      throw new Error("Gitlab query failed");
    }
    if (!r.length) {
      break;
    }
    result.push(...r);
  }

  return result;
}

/**
 * From gitlab API, get all merge requests between two dates
 */
interface MergeRequestInfo {
  iid: number;
  created_at: string;
  author: {
    name: string;
    username: string;
  };
}
async function getGitlabMergeRequestInfos(
  from: Date,
  to: Date
): Promise<MergeRequestInfo[]> {
  const c = await doOrCache("mr-ids", () => {
    return getGitlabPaginatedData<MergeRequestInfo>(
      // TODO: maybe state=merged
      `/projects/${GITLAB_PROJECT_ID}/merge_requests?state=all&scope=all&target_branch=master` +
        `&created_after=${from.toISOString()}&created_before=${to.toISOString()}`
    );
  });

  return c;
}

const APPROVAL_GRAPHQL = `query approvalRules($projectPath: ID!, $iid: String!) {
  project(fullPath: $projectPath) {
    id
    mergeRequest(iid: $iid) {
      id
      approvalState {
        invalidApproversRules {
          id
          __typename
        }
        rules {
          id
          type
          approved
          approvalsRequired
          invalid
          allowMergeWhenInvalid
          name
          section
          approvedBy {
            nodes {
              ...User
              __typename
            }
            __typename
          }
          commentedBy {
            nodes {
              ...User
              __typename
            }
            __typename
          }
          eligibleApprovers {
            ...User
            __typename
          }
          __typename
        }
        __typename
      }
      __typename
    }
    __typename
  }
}

fragment User on User {
  id
  avatarUrl
  name
  username
  webUrl
  __typename
}`;
interface GQResponse {
  data: {
    project: {
      mergeRequest: {
        approvalState: {
          rules: Array<{
            //  id: string; // ID of the "group"
            type: "ANY_APPROVER" | "REGULAR" | "CODE_OWNER";
            section: string;
            name: string;
            approvedBy: {
              nodes: Array<{ name: string }>;
            };
            eligibleApprovers: Array<{ name: string }>;
          }>;
        };
      };
    };
  };
}
function getGitlabApprovalFromMergeRequest(mrId: string): Promise<GQResponse> {
  return doOrCache(`${mrId}.approvals`, async () => {
    const queryJson = {
      operationName: "approvalRules",
      variables: {
        projectPath: GITLAB_PROJECT_PATH,
        iid: mrId,
      },
      query: APPROVAL_GRAPHQL,
    };

    const query = await fetch(GITLAB_GQL_URL, {
      headers: {
        "content-type": "application/json",
        Authorization: `Bearer ${process.env.GITLAB_TOKEN}`,
      },
      body: JSON.stringify(queryJson),
      method: "POST",
    });
    return (await query.json()) as GQResponse;
  });
}

interface MergeRequestApprovalsData {
  authors: Set<string>;
  date: Date;
  approvalGroups: Map<string, GroupData>;
  id: string;
}
async function getMergeRequestApprovalData(
  mr: MergeRequestInfo
): Promise<MergeRequestApprovalsData> {
  if (String(mr.iid) === "-8022") {
    console.log(mr);
    throw new Error();
  }
  const approvals = await getGitlabApprovalFromMergeRequest(String(mr.iid));
  return {
    id: String(mr.iid),
    date: new Date(mr.created_at),
    authors: new Set([mr.author.name]),
    approvalGroups: approvals.data.project.mergeRequest.approvalState.rules
      .filter((r) => r.type === "CODE_OWNER")
      .reduce((acc, cur) => {
        const set = acc.get(cur.section) ?? {
          members: new Set<string>(),
          rules: new Set<string>(),
        };
        acc.set(cur.section, set);
        cur.approvedBy.nodes.forEach((n) => set.members.add(n.name));
        cur.eligibleApprovers.forEach((n) => set.members.add(n.name));
        set.rules.add(cur.name);
        return acc;
      }, new Map<string, GroupData>()),
  };
}

interface GroupData {
  members: Set<string>;
  rules: Set<string>;
}
interface TeamsData {
  groups: Map<string, GroupData>;
  mrs: MergeRequestApprovalsData[];
}

function getTeamsData(
  data: MergeRequestApprovalsData[]
): Map<string, TeamsData> {
  const result = new Map<string, TeamsData>();

  for (const mergeRequestData of data) {
    const monthString = ("0" + (1 + mergeRequestData.date.getMonth())).slice(
      -2
    );
    const truncateDate = `${mergeRequestData.date.getFullYear()}-${monthString}`;
    const month: TeamsData = result.get(truncateDate) ?? {
      groups: new Map<string, GroupData>(),
      mrs: new Array<MergeRequestApprovalsData>(),
    };
    result.set(truncateDate, month);
    month.mrs.push(mergeRequestData);
    mergeRequestData.approvalGroups.forEach((mrApprovalGroup, groupId) => {
      const group = month.groups.get(groupId) ?? {
        members: new Set<string>(),
        rules: new Set<string>(),
      };
      month.groups.set(groupId, group);
      mrApprovalGroup.members.forEach((v) => group.members.add(v));
      mrApprovalGroup.rules.forEach((v) => group.rules.add(v));
    });
  }

  return result;
}

interface MergeRequestDependencyAnalysis {
  id: string;
  authorsGroup: Set<string>;
  requiredGroups: Map<string, Set<string>>; // map is reason => groups
}
function getMergeRequestTeamsDependencies(
  teamsData: Map<string, TeamsData>
): Map<string, MergeRequestDependencyAnalysis[]> {
  const mrDepAnalysis = new Map<string, MergeRequestDependencyAnalysis[]>();
  for (const [month, stat] of teamsData.entries()) {
    const reverseGroups = new Map<string, Set<string>>();
    for (const [groupName, group] of stat.groups.entries()) {
      for (const member of group.members.keys()) {
        const set = reverseGroups.get(member) ?? new Set<string>();
        reverseGroups.set(member, set);
        set.add(groupName);
      }
    }

    const v = new Array<MergeRequestDependencyAnalysis>();
    mrDepAnalysis.set(month, v);
    for (const mr of stat.mrs) {
      const requiredGroups = new Map<string, Set<string>>();
      const authorsGroup = new Set<string>();
      for (const author of mr.authors) {
        const groups = reverseGroups.get(author);
        if (groups) {
          for (const group of groups) {
            authorsGroup.add(group);
          }
        }
      }

      if (
        mr.authors.size === 0 ||
        (authorsGroup.size === 0 && mr.authors.size === 0)
      ) {
        continue;
      }
      if (mr.approvalGroups.size === 1 && authorsGroup.size === 0) {
        for (const v of mr.approvalGroups.keys()) {
          authorsGroup.add(v);
        }
      }

      if (authorsGroup.size === 0 && mr.approvalGroups.size > 0) {
        console.error(
          "Found a MR with no author group ",
          [...mr.authors],
          mr.id,
          [...mr.approvalGroups.keys()]
        );
        continue;
      }
      for (const [
        reviewerGroupKey,
        reviewerGroupVal,
      ] of mr.approvalGroups.entries()) {
        if (!authorsGroup.has(reviewerGroupKey)) {
          for (const rule of reviewerGroupVal.rules) {
            if (ignoreRules.has(rule)) {
              continue;
            }
            const dir = requiredGroups.get(rule) ?? new Set<string>();
            requiredGroups.set(rule, dir);
            dir.add(reviewerGroupKey);
          }
        }
      }
      v.push({ authorsGroup, requiredGroups, id: mr.id });
    }
  }
  return mrDepAnalysis;
}

interface SerializableGroup {
  members: string[];
  rules: string[];
}
interface SerializableTeamsData {
  groups: Record<string, SerializableGroup>;
}

function teamsDataToSerializable(
  data: Map<string, TeamsData>
): Record<string, SerializableTeamsData> {
  return [...data.entries()].reduce((acc, [month, teamsData]) => {
    acc[month] = {
      groups: [...teamsData.groups.entries()].reduce(
        (acc2, [groupId, group]) => {
          acc2[groupId] = {
            members: [...group.members].map((m) => getAnonymisedName(m)),
            rules: [...group.rules],
          };
          return acc2;
        },
        {} as Record<string, SerializableGroup>
      ),
    };
    return acc;
  }, {} as Record<string, SerializableTeamsData>);
}

interface SerializableMergeRequestDependencyAnalysis {
  id: string;
  authorsGroup: string[];
  requiredGroups: Record<string, string[]>;
}

function mergeRequestDependencyAnalysisToSerializable(
  data: Map<string, MergeRequestDependencyAnalysis[]>
): Record<string, SerializableMergeRequestDependencyAnalysis[]> {
  return [...data.entries()].reduce((acc, [month, analysis]) => {
    acc[month] = analysis.map((v) => ({
      id: v.id,
      authorsGroup: [...v.authorsGroup],
      requiredGroups: [...v.requiredGroups.entries()].reduce((acc2, [k, v]) => {
        acc2[k] = [...v];
        return acc2;
      }, {} as Record<string, string[]>),
    }));
    return acc;
  }, {} as Record<string, SerializableMergeRequestDependencyAnalysis[]>);
}

function toCsv(lines: Record<string, string | number>[]): string {
  const keyNamesToIndex = new Map<string, number>();

  for (const key of lines.flatMap((line) => Object.keys(line))) {
    if (!keyNamesToIndex.has(key)) {
      keyNamesToIndex.set(key, keyNamesToIndex.size);
    }
  }

  const header = [...keyNamesToIndex.keys()].join(",");
  const body = lines
    .map((line) => {
      const values = new Array<string>(keyNamesToIndex.size);
      for (let i = 0; i < values.length; ++i) {
        values[i] = "0";
      }
      for (const [key, value] of Object.entries(line)) {
        values[keyNamesToIndex.get(key)!] = String(value);
      }
      return values.join(",");
    })
    .join("\n");
  return `${header}\n${body}`;
}
async function main() {
  console.log("Getting MRs");
  const now = new Date(Date.now());
  const mrInfos = await getGitlabMergeRequestInfos(
    addDays(now, -TIME_PERIOD_DAYS),
    now
  );
  console.log(`Processing ${mrInfos.length} MRs`);
  let i = 0;
  const mrs = new Array<MergeRequestApprovalsData>();
  for (const info of mrInfos) {
    mrs.push(await getMergeRequestApprovalData(info));
    i++;
    console.log(`${Math.floor((1000 * i) / mrInfos.length) / 10}%`);
  }

  const teamsData = getTeamsData(mrs);
  await fs.writeFile(
    "teams.json",
    JSON.stringify(teamsDataToSerializable(teamsData))
  );

  const mrDepAnalysis = getMergeRequestTeamsDependencies(teamsData);
  await fs.writeFile(
    "mrDeps.json",
    JSON.stringify(mergeRequestDependencyAnalysisToSerializable(mrDepAnalysis))
  );

  // for each month, get the amount of MR that has additional group deps
  const mrDepsCount = [...mrDepAnalysis.entries()].reduce(
    (acc, [month, analysis]) => {
      acc[month] = {
        total: analysis.length,
        withDeps: analysis.filter((v) => v.requiredGroups.size > 0).length,
        countPerReason: analysis.reduce((countPerReasonAcc, analysis) => {
          const allGroups = [...analysis.requiredGroups.entries()].reduce(
            (acc, [reason, groups]) => {
              for (const x of groups) {
                acc.add(x);
              }
              return acc;
            },
            new Set<string>()
          );

          for (const grp of [...allGroups]) {
            const count = countPerReasonAcc[grp] ?? 0;
            countPerReasonAcc[grp] = count + 1;
          }

          return countPerReasonAcc;
        }, {} as Record<string, number>),
        ratio:
          analysis.length > 0
            ? (100 * analysis.filter((v) => v.requiredGroups.size > 0).length) /
              analysis.length
            : 0,
      };

      return acc;
    },
    {} as Record<
      string,
      {
        total: number;
        withDeps: number;
        ratio: number;
        countPerReason: Record<string, number>;
      }
    >
  );

  await fs.writeFile("mrDepsCount.json", JSON.stringify(mrDepsCount));

  const resultDataCsvArray = Object.entries(mrDepsCount).map(
    ([date, value]) => {
      const r: Record<string, string | number> = {};
      r["DATE"] = date;
      r["TOTAL"] = value.total;
      r["RATIO"] = value.ratio;
      for (const k of Object.keys(value.countPerReason)) {
        r[k] = (100 * value.countPerReason[k]) / value.total;
      }
      return r;
    }
  );
  await fs.writeFile("result.csv", toCsv(resultDataCsvArray));
}

main();
