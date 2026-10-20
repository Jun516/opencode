import path from "path"
import fs from "fs/promises"
import { expect } from "bun:test"
import { Agent } from "@opencode/core/agent"
import { Plugin } from "@opencode/core/plugin"
import { Config } from "@opencode/core/config"
import { ConfigAgentPlugin } from "@opencode/core/config/plugin/agent"
import { Bus } from "@opencode/core/bus"
import { Global } from "@opencode/util/global"
import { FSUtil } from "@opencode/util/fs-util"
import { Location } from "@opencode/core/location"
import { Credential } from "@opencode/core/credential"
import { WellKnown } from "@opencode/core/wellknown"
import { Watcher } from "@opencode/core/filesystem/watcher"
import { AppNodeBuilder } from "@opencode/core/effect/app-node-builder"
import { LayerNode } from "@opencode/util/effect/layer-node"
import { AbsolutePath } from "@opencode/core/schema"
import { Context, Effect, Fiber, Layer, Stream } from "effect"
import { testEffect } from "../lib/effect"
import { emptyCredentialNode, emptyWellknownNode } from "../fixture/config-nodes"
import { location } from "../fixture/location"
import { tmpdir } from "../fixture/tmpdir"
import { agentHost, host } from "../plugin/host"
import { PluginTestLayer } from "../plugin/fixture"

const it = testEffect(Layer.empty)
const managed = testEffect(PluginTestLayer)

it.live("probe real native agent add and edit discovery", () =>
  Effect.acquireDisposable(Effect.promise(() => tmpdir())).pipe(
    Effect.flatMap((tmp) =>
      Effect.gen(function* () {
        expect(Watcher.hasNativeBinding()).toBe(true)
        const project = path.join(tmp.path, "project")
        const directory = path.join(project, ".opencode", "agents")
        yield* Effect.promise(async () => {
          await fs.mkdir(directory, { recursive: true })
          await fs.mkdir(path.join(project, "global"))
          await fs.mkdir(path.join(project, "home"))
          await fs.writeFile(path.join(directory, "existing.md"), "Existing")
        })
        return yield* Effect.gen(function* () {
          const config = yield* Config.Service
          const agents = yield* Agent.Service
          const bus = yield* Bus.Service
          const updates: string[] = []
          yield* config.changes().pipe(
            Stream.runForEach((update) => Effect.sync(() => updates.push(JSON.stringify(update)))),
            Effect.forkScoped({ startImmediately: true }),
          )
          yield* ConfigAgentPlugin.Plugin.effect(
            host({ agent: agentHost(agents), event: { subscribe: () => bus.subscribe() } }),
          )
          expect(yield* agents.get(Agent.ID.make("existing"))).toMatchObject({ system: "Existing" })
          for (const [id, body] of [
            ["probe", "Created"],
            ["probe", "Updated"],
          ]) {
            const changed = yield* bus.subscribe(Agent.Event.Updated).pipe(
              Stream.filterEffect(() =>
                agents.get(Agent.ID.make(id)).pipe(Effect.map((agent) => agent?.system === body)),
              ),
              Stream.take(1),
              Stream.runDrain,
              Effect.forkScoped({ startImmediately: true }),
            )
            yield* Effect.promise(() => fs.writeFile(path.join(directory, id + ".md"), body))
            yield* Fiber.join(changed).pipe(
              Effect.timeout("10 seconds"),
              Effect.onError((error) => Effect.sync(() => console.error("PROBE", updates, error))),
            )
            expect(yield* agents.get(Agent.ID.make(id))).toMatchObject({ system: body })
          }
          console.log("NATIVE EVENTS", updates)
        }).pipe(
          Effect.provide(
            AppNodeBuilder.build(
              LayerNode.group([Config.node, Bus.node, Agent.node, Global.node, Location.node, FSUtil.node]),
              [
                Config.node.replace(Config.configured({ global: false })),
                Location.node.replace(
                  Layer.succeed(
                    Location.Service,
                    Location.Service.of(location({ directory: AbsolutePath.make(project) })),
                  ),
                ),
                Global.node.replace(
                  Global.layerWith({ config: path.join(project, "global"), home: path.join(project, "home") }),
                ),
                Credential.node.replace(emptyCredentialNode),
                WellKnown.node.replace(emptyWellknownNode),
              ],
            ),
          ),
        )
      }),
    ),
  ),
)

managed.live("probe real native discovery under managed activation context", () =>
  Effect.gen(function* () {
    expect(Watcher.hasNativeBinding()).toBe(true)
    const plugins = yield* Plugin.Service
    const agents = yield* Agent.Service
    const bus = yield* Bus.Service
    const tmp = yield* Effect.acquireDisposable(Effect.promise(() => tmpdir()))
    const project = path.join(tmp.path, "project")
    const directory = path.join(project, ".opencode", "agents")
    yield* Effect.promise(async () => {
      await fs.mkdir(directory, { recursive: true })
      await fs.mkdir(path.join(project, "global"))
      await fs.mkdir(path.join(project, "home"))
      await fs.writeFile(path.join(directory, "existing.md"), "Existing")
    })
    yield* Effect.gen(function* () {
      const context = Context.pick(
        Config.Service,
        FSUtil.Service,
        Global.Service,
        Location.Service,
      )(yield* Effect.context())
      yield* plugins.activate([
        {
          id: ConfigAgentPlugin.Plugin.id,
          revision: "1",
          effect: (ctx) => ConfigAgentPlugin.Plugin.effect(ctx).pipe(Effect.provide(context)),
        },
      ])
      expect(yield* agents.get(Agent.ID.make("existing"))).toMatchObject({ system: "Existing" })
      for (const [id, body] of [
        ["probe", "Created"],
        ["probe", "Updated"],
      ]) {
        const changed = yield* bus.subscribe(Agent.Event.Updated).pipe(
          Stream.filterEffect(() => agents.get(Agent.ID.make(id)).pipe(Effect.map((agent) => agent?.system === body))),
          Stream.take(1),
          Stream.runDrain,
          Effect.forkScoped({ startImmediately: true }),
        )
        yield* Effect.promise(() => fs.writeFile(path.join(directory, id + ".md"), body))
        yield* Fiber.join(changed).pipe(Effect.timeout("10 seconds"))
        expect(yield* agents.get(Agent.ID.make(id))).toMatchObject({ system: body })
      }
      yield* plugins.activate([])
    }).pipe(
      Effect.provide(
        AppNodeBuilder.build(LayerNode.group([Config.node, Bus.node, Global.node, Location.node, FSUtil.node]), [
          Config.node.replace(Config.configured({ global: false })),
          Location.node.replace(
            Layer.succeed(Location.Service, Location.Service.of(location({ directory: AbsolutePath.make(project) }))),
          ),
          Global.node.replace(
            Global.layerWith({ config: path.join(project, "global"), home: path.join(project, "home") }),
          ),
          Credential.node.replace(emptyCredentialNode),
          WellKnown.node.replace(emptyWellknownNode),
        ]),
      ),
    )
  }),
)
