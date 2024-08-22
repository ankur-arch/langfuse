import { z } from "zod";

import {
  datetimeFilterToPrismaSql,
  timeFilter,
  type ObservationOptions,
} from "@langfuse/shared";
import { protectedProjectProcedure } from "@/src/server/api/trpc";
import { Prisma } from "@langfuse/shared/src/db";
import { TypedSQL } from "@langfuse/shared/src/typedSql";

export const filterOptionsQuery = protectedProjectProcedure
  .input(
    z.object({ projectId: z.string(), startTimeFilter: timeFilter.optional() }),
  )
  .query(async ({ input, ctx }) => {
    const { startTimeFilter } = input;
    const prismaStartTimeFilter =
      startTimeFilter?.type === "datetime"
        ? startTimeFilter?.operator === ">="
          ? { gte: startTimeFilter.value }
          : startTimeFilter?.operator === ">"
            ? { gt: startTimeFilter.value }
            : startTimeFilter?.operator === "<="
              ? { lte: startTimeFilter.value }
              : startTimeFilter?.operator === "<"
                ? { lt: startTimeFilter.value }
                : {}
        : {};

    const queryFilter = {
      projectId: input.projectId,
      type: "GENERATION",
    } as const;

    // Score names
    const scores = await ctx.prisma.score.groupBy({
      where: {
        projectId: input.projectId,
        timestamp: prismaStartTimeFilter,
      },
      take: 1000,
      orderBy: {
        name: "desc",
      },
      by: ["name"],
    });

    // Model names
    const model = await ctx.prisma.observation.groupBy({
      by: ["model"],
      where: { ...queryFilter, startTime: prismaStartTimeFilter },
      _count: { _all: true },
      take: 1000,
      orderBy: {
        model: "desc",
      },
    });

    // Observation names
    const name = await ctx.prisma.observation.groupBy({
      by: ["name"],
      where: { ...queryFilter, startTime: prismaStartTimeFilter },
      _count: { _all: true },
      take: 1000,
      orderBy: {
        name: "desc",
      },
    });

    const promptNames = await ctx.prisma.$queryRawTyped(
      TypedSQL.promptNames(input.projectId),
    );
    // Trace names
    const rawStartTimeFilter =
      startTimeFilter && startTimeFilter.type === "datetime"
        ? datetimeFilterToPrismaSql(
            "start_time",
            startTimeFilter.operator,
            startTimeFilter.value,
          )
        : Prisma.empty;

    const traceName = await ctx.prisma.$queryRaw<
      Array<{
        traceName: string | null;
        count: number;
      }>
    >(Prisma.sql`
        SELECT
          t.name "traceName",
          count(*)::int AS count
        FROM traces t
        JOIN observations o ON o.trace_id = t.id
        WHERE o.type = 'GENERATION'
          AND o.project_id = ${input.projectId}
          AND t.project_id = ${input.projectId}
          ${rawStartTimeFilter}
        GROUP BY 1
        LIMIT 1000;
      `);

    // typecheck filter options, needs to include all columns with options
    const res: ObservationOptions = {
      model: model
        .filter((i) => i.model !== null)
        .map((i) => ({
          value: i.model as string,
          count: i._count._all,
        })),
      name: name
        .filter((i) => i.name !== null)
        .map((i) => ({
          value: i.name as string,
          count: i._count._all,
        })),
      traceName: traceName
        .filter((i) => i.traceName !== null)
        .map((i) => ({
          value: i.traceName as string,
          count: i.count,
        })),
      scores_avg: scores.map((score) => score.name),
      promptName: promptNames
        .filter((i) => i.promptname !== null)
        .map((i) => ({
          value: i.promptname as string,
          count: i?.count ?? 0,
        })),
    };

    return res;
  });
