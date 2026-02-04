import { Workflows } from '../src/workflows'
import { readWorkflowJson } from './utils/read-workflow-json'

describe('format', () => {
  it('format pipeline input and output', async () => {
    const workflows = Workflows.fromJson(readWorkflowJson('format.json'))
    workflows.bindModules({
      'format': () => import('./pipelines/format')
    })

    var pipelineOutput: any = undefined
    workflows.events.on(null, (pipeline) => {
      pipelineOutput = pipeline.output![0]
    })

    await workflows.run('format', 'a')

    expect(pipelineOutput).toMatchObject({ c: 1 })
  })
})