import { Workflows } from '../src/workflows'
import { readWorkflowJson } from './utils/read-workflow-json'

describe('output', () => {
  it('validate workflow output', async () => {
    const workflows = Workflows.fromJson(readWorkflowJson('output.json'))
    workflows.bindModules({
      'output': () => import('./pipelines/output')
    })

    var pipelineOutput: any = undefined
    workflows.events.on(null, (pipeline) => {
      if (pipeline.name == "output") {
        pipelineOutput = pipeline.input
      }
    })

    const workflowOutput = await workflows.run('output', 'a')
    expect(workflowOutput).toBe(pipelineOutput)
  })
})