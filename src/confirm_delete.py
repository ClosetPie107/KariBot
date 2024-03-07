from discord import ui, ButtonStyle
import discord

class ConfirmDeleteView(ui.View):
    def __init__(self, confirm_label, cancel_label,not_allowed_msg, initiator_id, *, timeout=60):
        super().__init__(timeout=timeout)
        self.value = None
        self.not_allowed_msg = not_allowed_msg
        self.initiator_id = initiator_id  # discord id of the user who initiated the command

        # Create confirm button
        confirm_button = ui.Button(label=confirm_label, style=ButtonStyle.red, custom_id='confirm_delete')
        confirm_button.callback = self.confirm
        self.add_item(confirm_button)

        # Create cancel button
        cancel_button = ui.Button(label=cancel_label, style=ButtonStyle.grey, custom_id='cancel_delete')
        cancel_button.callback = self.cancel

    async def confirm(self, interaction: discord.Interaction):
        # Check if the user who interacted with the button is the initiator
        if interaction.user.id == self.initiator_id:
            self.value = True
            self.stop()
            await interaction.response.defer(ephemeral=True)
        else:
            # if the user is not the initiator, send an error message
            await interaction.response.send_message(self.not_allowed_msg, ephemeral=True)

    async def cancel(self, interaction: discord.Interaction):
        # Check if the user who interacted with the button is the initiator
        if interaction.user.id == self.initiator_id:
            self.value = False
            self.stop()
            await interaction.response.defer(ephemeral=True)
        else:
            # if the user is not the initiator, send an error message
            await interaction.response.send_message(self.not_allowed_msg, ephemeral=True)