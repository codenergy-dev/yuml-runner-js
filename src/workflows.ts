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

  private async runNextPipeline(pipelines: Pipeline[], pipeline?: Pipeline, config?: PipelineRunConfig): Promise<void> {
    if (!pipeline) return;

    if (pipeline.path) {
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
          console.log(`\n▶️  ${pipeline.name}`)
          const inputWithArgs = { ...pipeline.input, ...pipeline.args }
          for (const [key, value] of Object.entries(inputWithArgs)) {
            console.log(`  └─ ${key}: ${value}`);
          }
          
          pipeline.state = PipelineState.EXEC;
          this.events.emit(pipeline)
          
          const pipelineFunction = await this.loadPipelineFunction(pipeline)
          pipeline.output = await pipelineFunction(inputWithArgs, config?.scope, config?.global);

          pipeline.state = PipelineState.DONE;
          pipeline.fanInCheck = [];
        } catch (e: any) {
          pipeline.output = null;
          pipeline.state = PipelineState.FAILED;
          pipeline.error = e.toString();
        } finally {
          this.events.emit(pipeline)
        }

        if (pipeline.error) {
          console.log(`\n⛔ ${pipeline.name}`);
          console.log(`  └─ ${pipeline.error}`);
          return;
        } else if (!pipeline.output) {
          console.log(`\n✅ ${pipeline.name}`);
          return;
        }

        if (Array.isArray(pipeline.output)) {
          console.log(`\n🔁 ${pipeline.name} (${pipeline.output.length})`);
        } else if (typeof pipeline.output === "object") {
          console.log(`\n✅ ${pipeline.name}`);
          pipeline.output = [pipeline.output];
        } else {
          throw new Error(`Unexpected output (${pipeline.output}) for pipeline ${pipeline.name}.`);
        }

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
            console.log(`\nℹ️  [${pipeline.name}]->[${nextPipeline.name}]`);
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
        console.log(`\nℹ️  [${nextPipeline.name}]<-[${pipeline.name}]`);
        nextPipeline.fanOutPending = pipeline.name;
      }

      await this.runNextPipeline(pipelines, nextPipeline, config);
    }
  }

  private async runNextPipelineFromPath(pipelines: Pipeline[], pipeline: Pipeline, config?: PipelineRunConfig): Promise<void> {
    var nextPipeline = pipelines
      .find(p => p.workflow == pipeline?.path && p.entrypoint)
    if (nextPipeline) {
      const nextPipelines = await this.run(nextPipeline.workflow, nextPipeline.name, config)
      nextPipeline = nextPipelines
        .find(p => p.workflow == pipeline?.path && p.entrypoint)
      if (nextPipeline) {
        pipeline.complete(nextPipeline.input, nextPipeline.args, nextPipeline.output)
      }
      
      const nextFanOut = nextPipelines
        .filter(p => pipeline.fanOut.includes(p.name))
      for (var fanOut of nextFanOut) {
        var nextPipelineFanOut = pipelines
          .find(p => pipeline.fanOut.includes(p.name)
                  && p.workflow == pipeline.workflow)
        if (nextPipelineFanOut) {
          nextPipelineFanOut.complete(fanOut.input, fanOut.args, fanOut.output)
          await this.runNextPipeline(pipelines, nextPipelineFanOut, config)
        }
      }
    }
  }

  private async loadPipelineFunction(pipeline: Pipeline) {
    const functionKey = `${pipeline.workflow}:${pipeline.functionName}`
    if (!this.functions[functionKey]) {
      const importedFunctions = await this.modules[pipeline.workflow]()
      for (var functionName in importedFunctions) {
        this.functions[`${pipeline.workflow}:${functionName}`] = importedFunctions[functionName] 
      }
    }
    if (!this.functions[functionKey]) {
      throw Error(`The function with name '${pipeline.functionName}' was not found in module '${pipeline.workflow}'.`)
    }
    return this.functions[functionKey]
  }
}