import {
  Pipeline,
  PipelineEventEmitter,
  PipelineFunctionMap,
  PipelineModuleMap,
  PipelineRunConfig,
  PipelineState,
} from "./pipeline";

export class Workflows {
  constructor(
    public readonly pipelines: Pipeline[],
    private modules: PipelineModuleMap = {},
  ) {
    this.pipelines
      .filter(p => p.path && p.entrypoint)
      .forEach(a => {
        this.events.on(a.functionName, (b) => {
          if (a.path == b.workflow) {
            this.run(a.workflow, a.name, {}, b)
          }
        })
      })
  }

  public readonly events: PipelineEventEmitter = new PipelineEventEmitter()
  
  private functions: PipelineFunctionMap = {}

  static fromJson(json: any[]) {
    const pipelines = json.map((pipeline) => Pipeline.fromJson(pipeline));
    return new Workflows(pipelines);
  }

  bindModules(modules: PipelineModuleMap) {
    this.modules = modules
  }

  async run(workflow: string, pipeline: string, config?: PipelineRunConfig, initialPipelineState?: Pipeline) {
    const pipelines = [...this.pipelines.map(p => p.reset())]
    const nextPipeline = pipelines
      .find(p => p.entrypoint
              && p.name == pipeline
              && p.workflow == workflow)
    if (nextPipeline && config?.args) {
      nextPipeline.input = nextPipeline.parseInput(config.args)
    }
    if (nextPipeline && initialPipelineState) {
      nextPipeline.copy(initialPipelineState)
    }
    await this.runNextPipeline(pipelines, nextPipeline, config)
    return pipelines
      .find(p => p.name == "output"
              && p.workflow == workflow
              && p.state == PipelineState.DONE)
      ?.input
  }

  private async runNextPipeline(
    pipelines: Pipeline[],
    pipeline?: Pipeline,
    config?: PipelineRunConfig,
    ignorePath: boolean = false,
  ): Promise<void> {
    if (!pipeline) return;

    if (pipeline.path && ignorePath == false) {
      return await this.runNextPipelineFromPath(pipelines, pipeline, config)
    }

    if (pipeline.isReady()) {
      if (pipeline.canBeExecuted()) {
        try {
          console.log(`▶️  ${pipeline}`)
          const inputWithArgs = pipeline.parseInput(pipeline.args, false)
          for (const [key, value] of Object.entries(inputWithArgs)) {
            console.log(`  └─ ${key}: ${value}`);
          }
          
          pipeline.state = PipelineState.EXEC;
          this.events.emit(pipeline, config)
          
          const pipelineFunction = pipeline.name == "output"
            ? () => null
            : await this.loadPipelineFunction(pipeline)
          const output = await pipelineFunction(inputWithArgs, config?.scope, config?.global);

          pipeline.state = PipelineState.DONE;
          pipeline.fanInCheck = [];

          if (Array.isArray(output)) {
            pipeline.output = output;
            console.log(`🔁 ${pipeline} (${output.length})`);
          } else if (typeof output === "object") {
            pipeline.output = [output];
            console.log(`✅ ${pipeline}`);
          } else if (!output) {
            pipeline.output = null;
            console.log(`🛑 ${pipeline}`);
          } else {
            throw new Error(`Unexpected output (${output}) for pipeline ${pipeline}.`);
          }
        } catch (e: any) {
          pipeline.output = null;
          pipeline.state = PipelineState.FAILED;
          pipeline.error = e.toString();
          console.log(`⚠️  ${pipeline} ${pipeline.error}`);
        } finally {
          this.events.emit(pipeline, config)
        }

        if (!pipeline.output || pipeline.name == "output") return;

        for (const output of pipeline.output) {
          if (typeof output === "object") {
            for (const [key, value] of Object.entries(output)) {
              console.log(`  └─ ${key}: ${value}`);
            }
          }
        }
      } else if (pipeline.state == PipelineState.SKIP) {
        console.log(`⏩ ${pipeline}`)
        this.events.emit(pipeline, config)
      }

      const fanOutList = pipeline.fanOutPending ? [pipeline.fanOutPending] : pipeline.fanOut;
      pipeline.fanOutPending = null;

      for (const output of pipeline.output!) {
        for (const fanOut of fanOutList) {
          const nextPipeline = pipelines.find(
            p => p.name === fanOut
              && p.workflow == pipeline.workflow
              && p.state != PipelineState.SKIP
          );
          if (!nextPipeline) continue;

          nextPipeline.state = PipelineState.IDLE;
          nextPipeline.input = nextPipeline.parseInput(output);

          if (nextPipeline.fanIn.includes(pipeline.name)) {
            nextPipeline.fanInCheck.push(pipeline.name);
          }
        }

        const nextPipelines: Promise<void>[] = []
        for (const fanOut of fanOutList) {
          const nextPipeline = pipelines.find(
            p => p.name === fanOut
              && [PipelineState.IDLE, PipelineState.WAIT, PipelineState.SKIP].includes(p.state)
              && p.workflow == pipeline.workflow
          );
          if (nextPipeline) {
            console.log(`⏭️  ${pipeline}->${nextPipeline}`);
            nextPipelines.push(this.runNextPipeline(pipelines, nextPipeline, config));
          }
        }
        await Promise.all(nextPipelines)
      }
    } else {
      pipeline.state = PipelineState.WAIT;
      this.events.emit(pipeline, config)

      const nextPipeline = pipelines.find(
        p => pipeline.fanIn.includes(p.name)
          && !pipeline.fanInCheck.includes(p.name)
          && p.workflow == pipeline.workflow
      );

      if (nextPipeline) {
        console.log(`⏸️  ${nextPipeline}<-${pipeline}`);
        nextPipeline.fanOutPending = pipeline.name;
      }

      await this.runNextPipeline(pipelines, nextPipeline, config);
    }
  }

  private async runNextPipelineFromPath(pipelines: Pipeline[], pipeline: Pipeline, config?: PipelineRunConfig): Promise<void> {
    var nextPipeline = pipelines
      .find(p => p.workflow == pipeline.path
              && p.functionName == pipeline.functionName)
    if (!nextPipeline) {
      throw new Error(`Pipeline from workflow '${pipeline.path}' with function '${pipeline.functionName}' not found.`);
    } else if (nextPipeline.entrypoint) {
      const nextPipelineRunConfig = {
        ...config,
        id: Date.now(),
        args: pipeline.parseInput(pipeline.args, false),
      }
      const nextPipelineCallback = (p: Pipeline, c?: PipelineRunConfig) => {
        if (c?.id != nextPipelineRunConfig.id) {
          return
        } else if (p.workflow == pipeline.path &&
                   p.functionName == pipeline.functionName &&
                   p.entrypoint) {
          pipeline.copy(p)
          pipeline.state = PipelineState.SKIP
        } else if (pipeline.fanOut.includes(`${p.workflow}.${p.name}`)) {
          const nextFanOut = pipelines
            .find(fanOut => fanOut.name == `${p.workflow}.${p.name}`
                         && fanOut.workflow == pipeline.workflow)
          if (nextFanOut) nextFanOut.copy(p)
          if (nextFanOut) nextFanOut.state = PipelineState.SKIP
        }
      }
      const nextPipelineUnsubscribe = this.events.on(null, nextPipelineCallback)
      await this.run(nextPipeline.workflow, nextPipeline.name, nextPipelineRunConfig)
      nextPipelineUnsubscribe()
    }
    
    await this.runNextPipeline(pipelines, pipeline, config, true)
  }

  private async loadPipelineFunction(pipeline: Pipeline) {
    const moduleName = pipeline.path ?? pipeline.workflow
    const functionKey = `${moduleName}.${pipeline.functionName}`
    if (!this.functions[functionKey]) {
      const importedFunctions = await this.modules[moduleName]()
      for (var functionName in importedFunctions) {
        this.functions[`${moduleName}.${functionName}`] = importedFunctions[functionName] 
      }
    }
    if (!this.functions[functionKey]) {
      throw Error(`The function with name '${pipeline.functionName}' was not found in module '${moduleName}'.`)
    }
    return this.functions[functionKey]
  }
}