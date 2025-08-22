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
  ) {}

  public readonly events: PipelineEventEmitter = new PipelineEventEmitter()
  
  private functions: PipelineFunctionMap = {}

  static fromJson(json: any[]) {
    const pipelines = json.map((pipeline) => Pipeline.fromJson(pipeline));
    return new Workflows(pipelines);
  }

  bindModules(modules: PipelineModuleMap) {
    this.modules = modules
  }

  async run(workflow: string, pipeline: string, config?: PipelineRunConfig) {
    const pipelines = [...this.pipelines.map(p => p.reset())]
    const nextPipeline = pipelines
      .find(p => p.entrypoint
              && p.name == pipeline
              && p.workflow == workflow)
    if (nextPipeline && config?.args) {
      nextPipeline.args = config.args
    }
    await this.runNextPipeline(pipelines, nextPipeline, config)
    return pipelines.filter(p => p.state == PipelineState.DONE || p.state == PipelineState.FAILED)
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

    const fanInSet = new Set(pipeline.fanIn);
    const fanInCheckSet = new Set(pipeline.fanInCheck);

    const isReady =
      fanInSet.size === fanInCheckSet.size &&
      [...fanInSet].every(value => fanInCheckSet.has(value));

    if (isReady) {
      if ([PipelineState.IDLE, PipelineState.WAIT].includes(pipeline.state)) {
        try {
          console.log(`\n▶️  ${pipeline}`)
          const inputWithArgs = { ...pipeline.input, ...pipeline.args }
          for (const [key, value] of Object.entries(inputWithArgs)) {
            console.log(`  └─ ${key}: ${value}`);
          }
          
          pipeline.state = PipelineState.EXEC;
          this.events.emit(pipeline)
          
          const pipelineFunction = await this.loadPipelineFunction(pipeline)
          const output = await pipelineFunction(inputWithArgs, config?.scope, config?.global);

          pipeline.state = PipelineState.DONE;
          pipeline.fanInCheck = [];

          if (Array.isArray(output)) {
            pipeline.output = output;
            console.log(`\n🔁 ${pipeline} (${output.length})`);
          } else if (typeof output === "object") {
            pipeline.output = [output];
            console.log(`\n✅ ${pipeline}`);
          } else if (!output) {
            pipeline.output = null;
            console.log(`\n✅ ${pipeline}`);
          } else {
            throw new Error(`Unexpected output (${output}) for pipeline ${pipeline}.`);
          }
        } catch (e: any) {
          pipeline.output = null;
          pipeline.state = PipelineState.FAILED;
          pipeline.error = e.toString();
          console.log(`\n⛔ ${pipeline}`);
          console.log(`  └─ ${pipeline.error}`);
        } finally {
          this.events.emit(pipeline)
        }

        if (!pipeline.output) return;

        for (const output of pipeline.output) {
          if (typeof output === "object") {
            for (const [key, value] of Object.entries(output)) {
              console.log(`  └─ ${key}: ${value}`);
            }
          }
        }
      }

      const fanOutList = pipeline.fanOutPending ? [pipeline.fanOutPending] : pipeline.fanOut;
      pipeline.fanOutPending = null;

      for (const output of pipeline.output!) {
        for (const fanOut of fanOutList) {
          const nextPipeline = pipelines.find(
            p => p.name === fanOut
              && p.workflow == pipeline.workflow
          );
          if (!nextPipeline) continue;

          nextPipeline.state = PipelineState.IDLE;
          nextPipeline.fanInCheck.push(pipeline.name);
          nextPipeline.input = { ...nextPipeline.input, ...output };
        }

        const nextPipelines: Promise<void>[] = []
        for (const fanOut of fanOutList) {
          const nextPipeline = pipelines.find(
            p => p.name === fanOut
              && [PipelineState.IDLE, PipelineState.WAIT].includes(p.state)
              && p.workflow == pipeline.workflow
          );
          if (nextPipeline) {
            console.log(`\nℹ️  ${pipeline}->${nextPipeline}`);
            nextPipelines.push(this.runNextPipeline(pipelines, nextPipeline, config));
          }
        }
        await Promise.all(nextPipelines)
      }
    } else {
      pipeline.state = PipelineState.WAIT;
      this.events.emit(pipeline)

      const nextPipeline = pipelines.find(
        p => pipeline.fanIn.includes(p.name)
          && !pipeline.fanInCheck.includes(p.name)
          && p.workflow == pipeline.workflow
      );

      if (nextPipeline) {
        console.log(`\nℹ️  ${nextPipeline}<-${pipeline}`);
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
      const nextPipelines = await this.run(nextPipeline.workflow, nextPipeline.name, config)
      nextPipeline = nextPipelines
        .find(p => p.workflow == pipeline.path
                && p.functionName == pipeline.functionName
                && p.entrypoint)
      if (nextPipeline) {
        pipeline.complete(nextPipeline.input, nextPipeline.args, nextPipeline.output)
      }
      
      const nextFanOut = nextPipelines
        .filter(p => pipeline.fanOut.includes(`${p.workflow}.${p.name}`))
      for (var fanOut of nextFanOut) {
        var nextPipelineFanOut = pipelines
          .find(p => pipeline.fanOut.includes(p.name)
                  && p.workflow == pipeline.workflow)
        if (nextPipelineFanOut) {
          nextPipelineFanOut.complete(fanOut.input, fanOut.args, fanOut.output)
          await this.runNextPipeline(pipelines, nextPipelineFanOut, config)
        }
      }
    } else {
      await this.runNextPipeline(pipelines, pipeline, config, true)
    }
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